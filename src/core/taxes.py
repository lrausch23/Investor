from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.broker_tax import augment_conn_ids_for_tax_rows, expand_ib_conn_ids, prefer_ib_offline_for_tax_rows
from src.core.connection_preference import preferred_active_connection_ids_for_scope
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerSymbolSummary,
    BrokerWashSaleEvent,
    HouseholdEntity,
    ExternalAccountMap,
    ExternalConnection,
    TaxDocument,
    TaxFact,
    TaxInput,
    TaxProfile,
    TaxTag,
    TaxpayerEntity,
    Transaction,
    IncomeEvent,
)


MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

TAX_TAG_CATEGORIES = [
    "IRA_DISTRIBUTION",
    "IRA_WITHHOLDING",
    "ESTIMATED_TAX_PAYMENT",
    "W2_WITHHOLDING",
    "TRUST_DISTRIBUTION",
    "BUSINESS_INCOME",
    "BUSINESS_EXPENSE",
]

TAX_TAG_LABELS = {
    "IRA_DISTRIBUTION": "IRA distribution",
    "IRA_WITHHOLDING": "IRA withholding",
    "ESTIMATED_TAX_PAYMENT": "Estimated tax payment",
    "W2_WITHHOLDING": "W-2 withholding",
    "TRUST_DISTRIBUTION": "Trust distribution",
    "BUSINESS_INCOME": "Business income",
    "BUSINESS_EXPENSE": "Business expense",
}

DOC_FACT_SOURCE_KEYS = {
    "w2_wages_total": ["WAGES"],
    "w2_withholding_total": ["FED_WITHHOLDING"],
    "ira_distributions_total": ["IRA_DISTRIBUTION"],
    "ira_withholding_total": ["IRA_WITHHOLDING"],
    "interest_total": ["INT_INCOME"],
    "dividends_ordinary_total": ["DIV_ORDINARY"],
    "dividends_qualified_total": ["DIV_QUALIFIED"],
    "cap_gain_dist_total": ["CAP_GAIN_DIST"],
    "k1_ordinary_total": ["K1_ORD_INCOME"],
    "k1_interest_total": ["K1_INTEREST"],
    "k1_dividends_total": ["K1_DIVIDENDS"],
    "k1_rental_total": ["K1_RENTAL"],
    "k1_other_total": ["K1_OTHER"],
    "k1_total": ["K1_ORD_INCOME", "K1_INTEREST", "K1_DIVIDENDS", "K1_RENTAL", "K1_OTHER"],
    "aca_premium_monthly": ["ACA_PREMIUM"],
    "aca_aptc_monthly": ["ACA_APTC"],
    "aca_slcsp_monthly": ["ACA_SLCSP"],
}


def _float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default


def _clamp_month_list(values: Any) -> list[float]:
    if not isinstance(values, list):
        return [0.0] * 12
    out = [_float(v) for v in values[:12]]
    while len(out) < 12:
        out.append(0.0)
    return out


def _deep_update(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in (updates or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_update(out[k], v)
        else:
            out[k] = v
    return out


def available_tax_years(session: Session, *, default_year: int | None = None, max_years: int = 10) -> list[int]:
    years: set[int] = set()
    for model, field in (
        (Transaction, Transaction.date),
        (BrokerLotClosure, BrokerLotClosure.trade_date),
        (IncomeEvent, IncomeEvent.date),
    ):
        try:
            min_date, max_date = session.query(func.min(field), func.max(field)).one()
        except Exception:
            continue
        if min_date and max_date:
            years.update(range(int(min_date.year), int(max_date.year) + 1))
    if default_year is None:
        default_year = dt.date.today().year
    years.add(int(default_year))
    if not years:
        years.update({int(default_year), int(default_year) - 1})
    years_list = sorted(years)
    if max_years and len(years_list) > max_years:
        years_list = years_list[-max_years:]
    return years_list


def _is_internal_transfer_like_links(links: dict[str, Any] | None) -> bool:
    links = links or {}
    desc = str(links.get("description") or "").upper()
    addl = str(links.get("additional_detail") or "").upper()
    raw = str(links.get("raw_type") or "").upper()
    txt = f"{desc} {addl}"
    if "DEPOSIT SWEEP" in txt:
        return True
    if "SHADO" in txt:
        return True
    if "REC FR SIS" in txt or "REC TRSF SIS" in txt:
        return True
    if "TRSF SIS" in txt:
        return True
    if raw == "UNKNOWN" and ("MULTI" in txt and "CURRENCY" in txt):
        return True
    if "FX" in txt and ("SETTLEMENT" in txt or "TRAD" in txt or "TRADE" in txt):
        return True
    return False


def is_internal_transfer_like(links: dict[str, Any] | None) -> bool:
    return _is_internal_transfer_like_links(links)


def _txn_text(tx: Transaction) -> str:
    links = tx.lot_links_json or {}
    parts = [
        str(links.get("description") or ""),
        str(links.get("additional_detail") or ""),
        str(tx.ticker or ""),
    ]
    return " ".join(p for p in parts if p).upper()


def _looks_like_withholding(text: str) -> bool:
    t = text.upper()
    return any(
        tok in t
        for tok in [
            "WITHHOLD",
            "WITHHOLDING",
            "W/H",
            "FEDERAL W/H",
            "STATE W/H",
            "FOREIGN TAX",
            "TAX WITHHOLD",
        ]
    )


def _is_div_int_withholding_tx(tx: Transaction) -> bool:
    try:
        amt = float(tx.amount or 0.0)
    except Exception:
        amt = 0.0
    if amt >= 0:
        return False
    text = _txn_text(tx)
    return _looks_like_withholding(text)


def _looks_like_dividend(text: str) -> bool:
    t = text.upper()
    return any(tok in t for tok in ["DIV", "DIVIDEND", "CASH DIV", "FOREIGN TAX WITHHELD", "ADR"])


def suggest_tax_tag(tx: Transaction, acct: Account, tp: TaxpayerEntity | None, *, trust_start: dt.date | None) -> str | None:
    acct_type = (acct.account_type or "").upper()
    tp_type = (tp.type or "").upper() if tp else ""
    if acct_type == "IRA":
        text = _txn_text(tx)
        if _looks_like_dividend(text):
            return None
        if _looks_like_withholding(text):
            return "IRA_WITHHOLDING"
        if tx.type in {"TRANSFER", "WITHHOLDING"} and not _is_internal_transfer_like_links(tx.lot_links_json):
            return "IRA_DISTRIBUTION"
        return None
    if tp_type == "TRUST":
        if trust_start and tx.date < trust_start:
            return None
        if tx.type == "TRANSFER" and float(tx.amount or 0.0) < 0 and not _is_internal_transfer_like_links(tx.lot_links_json):
            return "TRUST_DISTRIBUTION"
    return None


def auto_tag_tax_transactions(session: Session, *, year: int) -> int:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    rows = (
        session.query(Transaction, Account, TaxpayerEntity, TaxTag)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .outerjoin(TaxTag, TaxTag.transaction_id == Transaction.id)
        .filter(
            Transaction.date >= start,
            Transaction.date <= end,
            (Account.account_type == "IRA") | (TaxpayerEntity.type == "TRUST"),
        )
        .order_by(Transaction.date.asc(), Transaction.id.asc())
        .all()
    )
    created = 0
    updated = 0
    for tx, acct, tp, tag in rows:
        suggested = suggest_tax_tag(tx, acct, tp, trust_start=trust_start)
        if not suggested:
            continue
        if tag is None:
            session.add(TaxTag(transaction_id=tx.id, category=suggested, note="auto"))
            created += 1
            continue
        # Auto-correct IRA distribution vs withholding if previous auto-tag got it wrong.
        if suggested in {"IRA_WITHHOLDING", "IRA_DISTRIBUTION"} and tag.category in {"IRA_WITHHOLDING", "IRA_DISTRIBUTION"}:
            note_raw = "" if tag.note is None else str(tag.note).strip().lower()
            if tag.category != suggested and note_raw in {"", "auto", "none", "null"}:
                tag.category = suggested
                tag.note = "auto"
                tag.updated_at = dt.datetime.utcnow()
                updated += 1
    if created or updated:
        session.commit()
    return created + updated


def load_tax_params(year: int, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    base_path = Path("data") / "tax_params" / f"{int(year)}.json"
    fallback_path = Path("data") / "tax_params" / "2025.json"
    data: dict[str, Any] = {}
    path = base_path if base_path.exists() else fallback_path
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    if overrides and isinstance(overrides, dict):
        data = _deep_update(data, overrides)
    return data


def get_or_create_tax_profile(session: Session, *, year: int) -> TaxProfile:
    row = session.query(TaxProfile).filter(TaxProfile.year == int(year)).one_or_none()
    if row is None:
        row = TaxProfile(
            year=int(year),
            filing_status="MFJ",
            state_code=None,
            deductions_mode="standard",
            itemized_amount=None,
            household_size=3,
            dependents_count=1,
            trust_income_taxable_to_user=True,
        )
        session.add(row)
        session.commit()
    return row


def _default_inputs() -> dict[str, Any]:
    return {
        "yoga_net_profit_monthly": [0.0] * 12,
        "yoga_expense_ratio": 0.3,
        "daughter_w2_wages_monthly": [0.0] * 12,
        "daughter_w2_withholding_monthly": [0.0] * 12,
        "trust_passthrough_monthly": [0.0] * 12,
        "trust_fees_monthly": [0.0] * 12,
        "ira_withholding_override": None,
        "ira_distributions_override_monthly": [0.0] * 12,
        "estimated_payments": [],
        "aca_premium_monthly": [0.0] * 12,
        "aca_aptc_monthly": [0.0] * 12,
        "docs_primary": True,
        "tax_manual_overrides": {},
        "aca_enabled": True,
        "state_tax_rate": 0.0,
        "qualified_dividend_pct": 0.0,
        "niit_enabled": True,
        "niit_rate": 0.038,
        "last_year_total_tax": 0.0,
        "safe_harbor_multiplier": 1.0,
        "magi_override": None,
        "tax_doc_overrides": {
            "w2_wages_total": 0.0,
            "w2_withholding_total": 0.0,
            "ira_distributions_total": 0.0,
            "ira_distributions_gross_total": 0.0,
            "ira_withholding_total": 0.0,
            "interest_total": 0.0,
            "dividends_ordinary_total": 0.0,
            "dividends_qualified_total": 0.0,
            "cap_gain_dist_total": 0.0,
            "k1_ordinary_total": 0.0,
            "k1_interest_total": 0.0,
            "k1_dividends_total": 0.0,
            "k1_rental_total": 0.0,
            "k1_other_total": 0.0,
            "aca_premium_monthly": [0.0] * 12,
            "aca_aptc_monthly": [0.0] * 12,
            "aca_slcsp_monthly": [0.0] * 12,
            "sources": {},
            "by_entity": {},
        },
        "tax_parameter_overrides": {},
    }


def normalize_tax_inputs(data: dict[str, Any] | None) -> dict[str, Any]:
    base = _default_inputs()
    if isinstance(data, dict):
        base = _deep_update(base, data)
    for key in (
        "yoga_net_profit_monthly",
        "daughter_w2_wages_monthly",
        "daughter_w2_withholding_monthly",
        "trust_passthrough_monthly",
        "trust_fees_monthly",
        "ira_distributions_override_monthly",
        "aca_premium_monthly",
        "aca_aptc_monthly",
    ):
        base[key] = _clamp_month_list(base.get(key))
    base["state_tax_rate"] = _float(base.get("state_tax_rate"))
    base["qualified_dividend_pct"] = max(0.0, min(1.0, _float(base.get("qualified_dividend_pct"))))
    base["niit_rate"] = _float(base.get("niit_rate"))
    base["last_year_total_tax"] = _float(base.get("last_year_total_tax"))
    base["safe_harbor_multiplier"] = _float(base.get("safe_harbor_multiplier") or 1.0)
    base["aca_enabled"] = _bool(base.get("aca_enabled"), default=True)
    base["docs_primary"] = _bool(base.get("docs_primary"), default=True)
    manual_overrides = base.get("tax_manual_overrides") or {}
    if isinstance(manual_overrides, dict):
        for key in ("aca_premium_monthly", "aca_aptc_monthly", "aca_slcsp_monthly"):
            if key in manual_overrides:
                manual_overrides[key] = _clamp_month_list(manual_overrides.get(key))
    base["tax_manual_overrides"] = manual_overrides
    doc_overrides = base.get("tax_doc_overrides") or {}
    if isinstance(doc_overrides, dict):
        doc_overrides["aca_premium_monthly"] = _clamp_month_list(doc_overrides.get("aca_premium_monthly"))
        doc_overrides["aca_aptc_monthly"] = _clamp_month_list(doc_overrides.get("aca_aptc_monthly"))
        doc_overrides["aca_slcsp_monthly"] = _clamp_month_list(doc_overrides.get("aca_slcsp_monthly"))
    base["tax_doc_overrides"] = doc_overrides
    return base


def get_or_create_tax_inputs(session: Session, *, year: int) -> TaxInput:
    row = session.query(TaxInput).filter(TaxInput.year == int(year)).one_or_none()
    if row is None:
        row = TaxInput(year=int(year), data_json=_default_inputs())
        session.add(row)
        session.commit()
    return row


def _apply_brackets(amount: float, brackets: list[dict[str, Any]]) -> float:
    if amount <= 0:
        return 0.0
    taxable = amount
    prev_limit = 0.0
    tax = 0.0
    for bracket in brackets:
        rate = _float(bracket.get("rate"))
        limit = bracket.get("up_to")
        if limit is None:
            chunk = taxable
        else:
            limit_f = float(limit)
            chunk = max(0.0, min(taxable, limit_f - prev_limit))
        if chunk <= 0:
            prev_limit = float(limit) if limit is not None else prev_limit
            continue
        tax += chunk * rate
        taxable -= chunk
        prev_limit = float(limit) if limit is not None else prev_limit
        if taxable <= 0:
            break
    return tax


def _apply_ltcg_brackets(ordinary_taxable: float, ltcg_taxable: float, brackets: list[dict[str, Any]]) -> float:
    if ltcg_taxable <= 0:
        return 0.0
    remaining = ltcg_taxable
    used = 0.0
    tax = 0.0
    for bracket in brackets:
        rate = _float(bracket.get("rate"))
        limit = bracket.get("up_to")
        if limit is None:
            chunk = remaining
        else:
            limit_f = float(limit)
            capacity = max(0.0, limit_f - ordinary_taxable - used)
            chunk = min(remaining, capacity)
        if chunk <= 0:
            if limit is None:
                break
            continue
        tax += chunk * rate
        remaining -= chunk
        used += chunk
        if remaining <= 0:
            break
    return tax


def compute_se_tax(net_profit: float, params: dict[str, Any], filing_status: str) -> tuple[float, float]:
    if net_profit <= 0:
        return 0.0, 0.0
    se_taxable = net_profit * 0.9235
    se_cfg = params.get("se_tax") if isinstance(params, dict) else {}
    ss_rate = _float(se_cfg.get("ss_rate") or 0.124)
    medicare_rate = _float(se_cfg.get("medicare_rate") or 0.029)
    addl_rate = _float(se_cfg.get("additional_medicare_rate") or 0.009)
    ss_base = _float(se_cfg.get("ss_wage_base") or 168600)
    ss_taxable = min(se_taxable, ss_base)
    ss_tax = ss_taxable * ss_rate
    medicare_tax = se_taxable * medicare_rate
    addl_threshold = 200000.0
    filing_cfg = params.get("filing_status", {}).get(filing_status.upper(), {}) if isinstance(params, dict) else {}
    if filing_cfg and filing_cfg.get("additional_medicare_threshold") is not None:
        addl_threshold = _float(filing_cfg.get("additional_medicare_threshold"))
    addl_tax = max(0.0, se_taxable - addl_threshold) * addl_rate
    se_tax = ss_tax + medicare_tax + addl_tax
    se_deduction = se_tax * 0.5
    return se_tax, se_deduction


def _term_from_open_date(trade_date: dt.date, open_date_raw: str | None) -> str:
    if not open_date_raw:
        return "UNKNOWN"
    s = str(open_date_raw)
    if ";" in s:
        s = s.split(";", 1)[0]
    if len(s) >= 8 and s[:8].isdigit():
        try:
            od = dt.datetime.strptime(s[:8], "%Y%m%d").date()
        except Exception:
            return "UNKNOWN"
    else:
        try:
            od = dt.date.fromisoformat(s[:10])
        except Exception:
            return "UNKNOWN"
    return "LT" if (trade_date - od).days >= 365 else "ST"


def _year_bounds(year: int) -> tuple[dt.date, dt.date]:
    return dt.date(year, 1, 1), dt.date(year, 12, 31)


def _trust_start_for_year(year: int) -> dt.date | None:
    return dt.date(2025, 6, 6) if int(year) == 2025 else None


def _account_ids_by_category(session: Session) -> dict[str, list[int]]:
    rows = (
        session.query(Account.id, Account.account_type, TaxpayerEntity.type)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .all()
    )
    out = {
        "trust": [],
        "non_trust": [],
        "ira": [],
        "trust_ira": [],
        "trust_non_ira": [],
        "non_trust_non_ira": [],
    }
    for acct_id, acct_type, tp_type in rows:
        is_trust = str(tp_type or "").upper() == "TRUST"
        is_ira = str(acct_type or "").upper() == "IRA"
        if is_trust:
            out["trust"].append(int(acct_id))
            if is_ira:
                out["trust_ira"].append(int(acct_id))
            else:
                out["trust_non_ira"].append(int(acct_id))
        else:
            out["non_trust"].append(int(acct_id))
            if not is_ira:
                out["non_trust_non_ira"].append(int(acct_id))
        if is_ira:
            out["ira"].append(int(acct_id))
    return out


def _pass_through_labels(session: Session, *, year: int) -> set[str]:
    names: set[str] = set()
    acct_rows = (
        session.query(Account, TaxpayerEntity)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .all()
    )
    for acct, tp in acct_rows:
        acct_name = (acct.name or "").strip()
        if not acct_name:
            continue
        if str(tp.type or "").upper() == "TRUST":
            names.add(acct_name)
            continue
        if "LLC" in acct_name.upper().replace(".", ""):
            names.add(acct_name)

    ent_rows = session.query(HouseholdEntity).filter(HouseholdEntity.tax_year == int(year)).all()
    for ent in ent_rows:
        label = (ent.display_name or "").strip()
        if not label:
            continue
        if str(ent.entity_type or "").upper() in {"TRUST", "BUSINESS"}:
            names.add(label)
            continue
        if "LLC" in label.upper().replace(".", ""):
            names.add(label)
    return names


def _connection_ids_for_household(session: Session, *, include_trust: bool, start: dt.date, end: dt.date) -> list[int]:
    ids = preferred_active_connection_ids_for_scope(session, scope="household")
    if include_trust is False:
        conn_rows = (
            session.query(ExternalConnection.id)
            .join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
            .filter(ExternalConnection.id.in_(ids), TaxpayerEntity.type != "TRUST")
            .all()
        )
        ids = [int(r[0]) for r in conn_rows]
    ids = augment_conn_ids_for_tax_rows(session, conn_ids=ids, start=start, end=end)
    ids = expand_ib_conn_ids(session, conn_ids=ids)
    return prefer_ib_offline_for_tax_rows(session, conn_ids=ids, start=start, end=end)


def _tagged_amounts_by_month(
    session: Session,
    *,
    category: str,
    start: dt.date,
    end: dt.date,
    non_trust_account_ids: list[int],
    trust_account_ids: list[int] | None = None,
    trust_start: dt.date | None = None,
    sign: str = "abs",
) -> list[float]:
    out = [0.0] * 12
    trust_ids = trust_account_ids or []

    def _apply_rows(rows: list[tuple[Transaction, TaxTag]]) -> None:
        for tx, _tag in rows:
            m = int(tx.date.month) - 1
            amt = float(tx.amount or 0.0)
            if sign == "abs":
                amt = abs(amt)
            out[m] += amt

    if non_trust_account_ids:
        rows = (
            session.query(Transaction, TaxTag)
            .join(TaxTag, TaxTag.transaction_id == Transaction.id)
            .filter(
                TaxTag.category == category,
                Transaction.account_id.in_(non_trust_account_ids),
                Transaction.date >= start,
                Transaction.date <= end,
            )
            .all()
        )
        _apply_rows(rows)

    if trust_ids:
        trust_begin = trust_start or start
        rows = (
            session.query(Transaction, TaxTag)
            .join(TaxTag, TaxTag.transaction_id == Transaction.id)
            .filter(
                TaxTag.category == category,
                Transaction.account_id.in_(trust_ids),
                Transaction.date >= trust_begin,
                Transaction.date <= end,
            )
            .all()
        )
        _apply_rows(rows)
    return out


def _ira_tax_flows_by_month(
    session: Session,
    *,
    start: dt.date,
    end: dt.date,
    account_ids: list[int],
) -> tuple[list[float], list[float]]:
    dist = [0.0] * 12
    withh = [0.0] * 12
    if not account_ids:
        return dist, withh
    rows = (
        session.query(Transaction, Account, TaxpayerEntity)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            Transaction.account_id.in_(account_ids),
            Transaction.date >= start,
            Transaction.date <= end,
        )
        .all()
    )
    for tx, acct, tp in rows:
        suggested = suggest_tax_tag(tx, acct, tp, trust_start=None)
        if suggested is None:
            continue
        m = int(tx.date.month) - 1
        amt = abs(float(tx.amount or 0.0))
        if suggested == "IRA_DISTRIBUTION":
            dist[m] += amt
        elif suggested == "IRA_WITHHOLDING":
            withh[m] += amt
    return dist, withh


def _income_events_by_month(
    session: Session,
    *,
    start: dt.date,
    end: dt.date,
    non_trust_account_ids: list[int],
    trust_account_ids: list[int] | None = None,
    trust_start: dt.date | None = None,
    types: tuple[str, ...],
) -> list[float]:
    out = [0.0] * 12
    trust_ids = trust_account_ids or []

    def _apply_rows(rows: list[IncomeEvent]) -> None:
        for ev in rows:
            m = int(ev.date.month) - 1
            out[m] += float(ev.amount or 0.0)

    if non_trust_account_ids:
        rows = (
            session.query(IncomeEvent)
            .filter(
                IncomeEvent.account_id.in_(non_trust_account_ids),
                IncomeEvent.date >= start,
                IncomeEvent.date <= end,
                IncomeEvent.type.in_(types),
            )
            .all()
        )
        _apply_rows(rows)

    if trust_ids:
        trust_begin = trust_start or start
        rows = (
            session.query(IncomeEvent)
            .filter(
                IncomeEvent.account_id.in_(trust_ids),
                IncomeEvent.date >= trust_begin,
                IncomeEvent.date <= end,
                IncomeEvent.type.in_(types),
            )
            .all()
        )
        _apply_rows(rows)
    return out


def _income_event_account_ids(
    session: Session,
    *,
    account_ids: list[int],
    start: dt.date,
    end: dt.date,
    types: tuple[str, ...],
) -> set[int]:
    if not account_ids:
        return set()
    rows = (
        session.query(IncomeEvent.account_id)
        .filter(
            IncomeEvent.account_id.in_(account_ids),
            IncomeEvent.date >= start,
            IncomeEvent.date <= end,
            IncomeEvent.type.in_(types),
        )
        .distinct()
        .all()
    )
    return {int(r[0]) for r in rows}


def _transaction_div_int_by_month(
    session: Session,
    *,
    start: dt.date,
    end: dt.date,
    non_trust_account_ids: list[int],
    trust_account_ids: list[int] | None = None,
    trust_start: dt.date | None = None,
    types: tuple[str, ...] = ("DIV", "INT"),
    exclude_withholding: bool = False,
) -> list[float]:
    out = [0.0] * 12
    trust_ids = trust_account_ids or []
    non_trust_income_ids = _income_event_account_ids(
        session,
        account_ids=non_trust_account_ids,
        start=start,
        end=end,
        types=("DIVIDEND",) if types == ("DIV",) else ("INTEREST",) if types == ("INT",) else ("DIVIDEND", "INTEREST"),
    )
    trust_begin = trust_start or start
    trust_income_ids = _income_event_account_ids(
        session,
        account_ids=trust_ids,
        start=trust_begin,
        end=end,
        types=("DIVIDEND",) if types == ("DIV",) else ("INTEREST",) if types == ("INT",) else ("DIVIDEND", "INTEREST"),
    )

    def _apply_rows(rows: list[Transaction]) -> None:
        seen: set[tuple[int, dt.date, float, str]] = set()
        for tx in rows:
            if exclude_withholding and _is_div_int_withholding_tx(tx):
                continue
            desc = (tx.lot_links_json or {}).get("description") or tx.ticker or ""
            key = (int(tx.account_id), tx.date, float(tx.amount or 0.0), desc)
            if key in seen:
                continue
            seen.add(key)
            m = int(tx.date.month) - 1
            out[m] += float(tx.amount or 0.0)

    if non_trust_account_ids:
        q = session.query(Transaction).filter(
            Transaction.account_id.in_(non_trust_account_ids),
            Transaction.date >= start,
            Transaction.date <= end,
            Transaction.type.in_(types),
        )
        if non_trust_income_ids:
            q = q.filter(~Transaction.account_id.in_(list(non_trust_income_ids)))
        rows = q.all()
        _apply_rows(rows)

    if trust_ids:
        q = session.query(Transaction).filter(
            Transaction.account_id.in_(trust_ids),
            Transaction.date >= trust_begin,
            Transaction.date <= end,
            Transaction.type.in_(types),
        )
        if trust_income_ids:
            q = q.filter(~Transaction.account_id.in_(list(trust_income_ids)))
        rows = q.all()
        _apply_rows(rows)
    return out


def _withholding_by_month(
    session: Session,
    *,
    start: dt.date,
    end: dt.date,
    non_trust_account_ids: list[int],
    trust_account_ids: list[int] | None = None,
    trust_start: dt.date | None = None,
) -> list[float]:
    out = [0.0] * 12
    trust_ids = trust_account_ids or []

    def _apply_tx_rows(rows: list[Transaction]) -> None:
        for tx in rows:
            m = int(tx.date.month) - 1
            out[m] += abs(float(tx.amount or 0.0))

    def _apply_income_rows(rows: list[IncomeEvent]) -> None:
        for ev in rows:
            m = int(ev.date.month) - 1
            out[m] += abs(float(ev.amount or 0.0))

    def _apply_div_int_withholding(rows: list[Transaction]) -> None:
        for tx in rows:
            if not _is_div_int_withholding_tx(tx):
                continue
            m = int(tx.date.month) - 1
            out[m] += abs(float(tx.amount or 0.0))

    if non_trust_account_ids:
        tx_rows = (
            session.query(Transaction)
            .filter(
                Transaction.account_id.in_(non_trust_account_ids),
                Transaction.date >= start,
                Transaction.date <= end,
                Transaction.type == "WITHHOLDING",
            )
            .all()
        )
        _apply_tx_rows(tx_rows)

        ev_rows = (
            session.query(IncomeEvent)
            .filter(
                IncomeEvent.account_id.in_(non_trust_account_ids),
                IncomeEvent.date >= start,
                IncomeEvent.date <= end,
                IncomeEvent.type == "WITHHOLDING",
            )
            .all()
        )
        _apply_income_rows(ev_rows)

        div_int_rows = (
            session.query(Transaction)
            .filter(
                Transaction.account_id.in_(non_trust_account_ids),
                Transaction.date >= start,
                Transaction.date <= end,
                Transaction.type.in_(("DIV", "INT")),
                Transaction.amount < 0,
            )
            .all()
        )
        _apply_div_int_withholding(div_int_rows)

    if trust_ids:
        trust_begin = trust_start or start
        tx_rows = (
            session.query(Transaction)
            .filter(
                Transaction.account_id.in_(trust_ids),
                Transaction.date >= trust_begin,
                Transaction.date <= end,
                Transaction.type == "WITHHOLDING",
            )
            .all()
        )
        _apply_tx_rows(tx_rows)

        ev_rows = (
            session.query(IncomeEvent)
            .filter(
                IncomeEvent.account_id.in_(trust_ids),
                IncomeEvent.date >= trust_begin,
                IncomeEvent.date <= end,
                IncomeEvent.type == "WITHHOLDING",
            )
            .all()
        )
        _apply_income_rows(ev_rows)

        div_int_rows = (
            session.query(Transaction)
            .filter(
                Transaction.account_id.in_(trust_ids),
                Transaction.date >= trust_begin,
                Transaction.date <= end,
                Transaction.type.in_(("DIV", "INT")),
                Transaction.amount < 0,
            )
            .all()
        )
        _apply_div_int_withholding(div_int_rows)
    return out


def _capital_gains_by_month(
    session: Session,
    *,
    conn_ids: list[int],
    start: dt.date,
    end: dt.date,
    include_trust: bool,
    trust_start: dt.date | None,
) -> tuple[list[float], list[float]]:
    rows = (
        session.query(BrokerLotClosure, ExternalConnection, TaxpayerEntity)
        .join(ExternalConnection, ExternalConnection.id == BrokerLotClosure.connection_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
        .filter(
            BrokerLotClosure.connection_id.in_(conn_ids),
            BrokerLotClosure.trade_date >= start,
            BrokerLotClosure.trade_date <= end,
        )
        .all()
    )
    st = [0.0] * 12
    lt = [0.0] * 12
    for cl, _conn, tp in rows:
        if not include_trust and (tp.type or "").upper() == "TRUST":
            continue
        if trust_start and (tp.type or "").upper() == "TRUST" and cl.trade_date < trust_start:
            continue
        term = _term_from_open_date(cl.trade_date, cl.open_datetime_raw)
        realized = float(cl.realized_pl_fifo or 0.0)
        idx = int(cl.trade_date.month) - 1
        if term == "LT":
            lt[idx] += realized
        else:
            st[idx] += realized
    return st, lt


def _apply_symbol_summary_adjustment(
    session: Session, *, year: int, st_gains: list[float], lt_gains: list[float]
) -> tuple[list[float], list[float]]:
    summary = capital_gains_summary_by_account(session, year=year)
    if not summary:
        return st_gains, lt_gains
    if not any(row.get("symbol_summary_used") for row in summary):
        return st_gains, lt_gains

    adjusted_total = sum(float(row.get("realized_with_wash") or 0.0) for row in summary)
    fifo_total = sum(st_gains) + sum(lt_gains)
    if abs(adjusted_total - fifo_total) < 0.01:
        return st_gains, lt_gains
    if abs(fifo_total) < 1e-9:
        st = [0.0] * 12
        lt = [0.0] * 12
        st[11] = adjusted_total
        return st, lt

    factor = adjusted_total / fifo_total
    st = [v * factor for v in st_gains]
    lt = [v * factor for v in lt_gains]
    return st, lt


def compute_tax_breakdown(
    *,
    ordinary_core: float,
    st_gains: float,
    lt_gains: float,
    dividends: float,
    qualified_dividend_pct: float,
    deductions: float,
    se_deduction: float,
    filing_status: str,
    params: dict[str, Any],
    niit_enabled: bool,
    niit_rate: float,
) -> dict[str, Any]:
    filing_cfg = params.get("filing_status", {}).get(filing_status.upper(), {}) if isinstance(params, dict) else {}
    ordinary_brackets = filing_cfg.get("ordinary_brackets", [])
    ltcg_brackets = filing_cfg.get("ltcg_brackets", [])

    qualified_dividends = dividends * qualified_dividend_pct
    nonqualified_dividends = max(0.0, dividends - qualified_dividends)

    ordinary_base = ordinary_core + st_gains + nonqualified_dividends
    ltcg_base = lt_gains + qualified_dividends

    deduction_total = max(0.0, deductions + se_deduction)
    ordinary_taxable = max(0.0, ordinary_base - deduction_total)
    leftover_deduction = max(0.0, deduction_total - ordinary_base)
    ltcg_taxable = max(0.0, ltcg_base - leftover_deduction)

    ordinary_tax = _apply_brackets(ordinary_taxable, ordinary_brackets)
    ltcg_tax = _apply_ltcg_brackets(ordinary_taxable, ltcg_taxable, ltcg_brackets)

    niit_tax = 0.0
    if niit_enabled:
        niit_base = max(0.0, st_gains + lt_gains + dividends)
        niit_tax = niit_base * niit_rate

    return {
        "ordinary_taxable": ordinary_taxable,
        "ltcg_taxable": ltcg_taxable,
        "ordinary_tax": ordinary_tax,
        "ltcg_tax": ltcg_tax,
        "niit_tax": niit_tax,
        "qualified_dividends": qualified_dividends,
        "nonqualified_dividends": nonqualified_dividends,
    }


@dataclass(frozen=True)
class TaxDashboard:
    year: int
    profile: TaxProfile
    inputs: dict[str, Any]
    params: dict[str, Any]
    summary: dict[str, Any]
    monthly: list[dict[str, Any]]
    tags: dict[str, Any]


def build_tax_dashboard(
    session: Session,
    *,
    year: int,
    as_of: dt.date | None = None,
    apply_overrides: bool = True,
) -> TaxDashboard:
    profile = get_or_create_tax_profile(session, year=year)
    inputs_row = get_or_create_tax_inputs(session, year=year)
    inputs = normalize_tax_inputs(dict(inputs_row.data_json or {}))
    params = load_tax_params(year, overrides=inputs.get("tax_parameter_overrides"))

    start, end = _year_bounds(year)
    if as_of is None:
        today = dt.date.today()
        as_of = min(end, today)

    include_trust = bool(profile.trust_income_taxable_to_user)
    acct_ids = _account_ids_by_category(session)
    trust_account_ids = acct_ids["trust"]
    non_trust_account_ids = acct_ids["non_trust"]
    trust_non_ira_ids = acct_ids["trust_non_ira"]
    non_trust_non_ira_ids = acct_ids["non_trust_non_ira"]
    if not include_trust:
        trust_account_ids = []
        trust_non_ira_ids = []
    allowed_account_ids = non_trust_account_ids + trust_account_ids

    trust_start = _trust_start_for_year(year)
    conn_ids = _connection_ids_for_household(session, include_trust=include_trust, start=start, end=end)

    ira_distributions, ira_withholding = _ira_tax_flows_by_month(
        session,
        start=start,
        end=end,
        account_ids=acct_ids["ira"],
    )
    ira_distributions_override = _clamp_month_list(inputs.get("ira_distributions_override_monthly"))
    if sum(ira_distributions_override) > 0:
        ira_distributions = ira_distributions_override
    ira_withholding_override = inputs.get("ira_withholding_override")
    if ira_withholding_override not in (None, ""):
        override_val = _float(ira_withholding_override)
        ira_withholding = [override_val / 12.0] * 12

    ira_distributions_gross = [ira_distributions[i] + ira_withholding[i] for i in range(12)]
    estimated_payments_tagged = _tagged_amounts_by_month(
        session,
        category="ESTIMATED_TAX_PAYMENT",
        start=start,
        end=end,
        non_trust_account_ids=non_trust_account_ids,
        trust_account_ids=trust_account_ids,
        trust_start=trust_start,
        sign="abs",
    )
    w2_withholding_tagged = _tagged_amounts_by_month(
        session,
        category="W2_WITHHOLDING",
        start=start,
        end=end,
        non_trust_account_ids=non_trust_account_ids,
        trust_account_ids=trust_account_ids,
        trust_start=trust_start,
        sign="abs",
    )

    other_withholding = _withholding_by_month(
        session,
        start=start,
        end=end,
        non_trust_account_ids=non_trust_non_ira_ids,
        trust_account_ids=trust_non_ira_ids,
        trust_start=trust_start,
    )
    business_income_tagged = _tagged_amounts_by_month(
        session,
        category="BUSINESS_INCOME",
        start=start,
        end=end,
        non_trust_account_ids=non_trust_account_ids,
        trust_account_ids=trust_account_ids,
        trust_start=trust_start,
        sign="abs",
    )
    business_expense_tagged = _tagged_amounts_by_month(
        session,
        category="BUSINESS_EXPENSE",
        start=start,
        end=end,
        non_trust_account_ids=non_trust_account_ids,
        trust_account_ids=trust_account_ids,
        trust_start=trust_start,
        sign="abs",
    )

    interest_income = _income_events_by_month(
        session,
        start=start,
        end=end,
        non_trust_account_ids=non_trust_non_ira_ids,
        trust_account_ids=trust_non_ira_ids,
        trust_start=trust_start,
        types=("INTEREST",),
    )
    dividend_income = _income_events_by_month(
        session,
        start=start,
        end=end,
        non_trust_account_ids=non_trust_non_ira_ids,
        trust_account_ids=trust_non_ira_ids,
        trust_start=trust_start,
        types=("DIVIDEND",),
    )
    fallback_div = _transaction_div_int_by_month(
        session,
        start=start,
        end=end,
        non_trust_account_ids=non_trust_non_ira_ids,
        trust_account_ids=trust_non_ira_ids,
        trust_start=trust_start,
        types=("DIV",),
        exclude_withholding=True,
    )
    fallback_int = _transaction_div_int_by_month(
        session,
        start=start,
        end=end,
        non_trust_account_ids=non_trust_non_ira_ids,
        trust_account_ids=trust_non_ira_ids,
        trust_start=trust_start,
        types=("INT",),
        exclude_withholding=True,
    )
    dividend_income = [dividend_income[i] + fallback_div[i] for i in range(12)]
    interest_income = [interest_income[i] + fallback_int[i] for i in range(12)]

    st_gains, lt_gains = _capital_gains_by_month(
        session,
        conn_ids=conn_ids,
        start=start,
        end=end,
        include_trust=include_trust,
        trust_start=trust_start,
    )
    st_gains, lt_gains = _apply_symbol_summary_adjustment(session, year=year, st_gains=st_gains, lt_gains=lt_gains)

    yoga_net_profit = _clamp_month_list(inputs.get("yoga_net_profit_monthly"))
    yoga_ratio = _float(inputs.get("yoga_expense_ratio"))
    if sum(abs(v) for v in yoga_net_profit) == 0.0 and sum(business_income_tagged) > 0:
        net = []
        for i in range(12):
            gross = business_income_tagged[i]
            expense = business_expense_tagged[i]
            if expense > 0:
                net.append(gross - expense)
            else:
                net.append(gross * (1.0 - max(0.0, min(1.0, yoga_ratio))))
        yoga_net_profit = net

    w2_wages = _clamp_month_list(inputs.get("daughter_w2_wages_monthly"))
    w2_withholding = _clamp_month_list(inputs.get("daughter_w2_withholding_monthly"))
    trust_passthrough_gross = _clamp_month_list(inputs.get("trust_passthrough_monthly")) if include_trust else [0.0] * 12
    trust_fees = _clamp_month_list(inputs.get("trust_fees_monthly")) if include_trust else [0.0] * 12
    trust_passthrough = [max(0.0, trust_passthrough_gross[i] - trust_fees[i]) for i in range(12)]
    aca_premium = _clamp_month_list(inputs.get("aca_premium_monthly"))
    aca_aptc = _clamp_month_list(inputs.get("aca_aptc_monthly"))

    dividends_monthly = dividend_income
    interest_monthly = interest_income

    k1_monthly = [0.0] * 12
    doc_qual_override: float | None = None
    source_map: dict[str, str] = {}
    manual_overrides = inputs.get("tax_manual_overrides") or {}
    doc_overrides = inputs.get("tax_doc_overrides") or {}
    if not isinstance(doc_overrides, dict):
        doc_overrides = {}
    if apply_overrides and _bool(inputs.get("docs_primary"), default=True):
        if doc_overrides.get("sources") and _float(doc_overrides.get("ira_distributions_gross_total")) == 0.0:
            try:
                from src.core.tax_documents import aggregate_tax_doc_overrides

                refreshed = aggregate_tax_doc_overrides(session, tax_year=year)
                if isinstance(refreshed, dict) and refreshed.get("sources"):
                    doc_overrides = refreshed
                    inputs["tax_doc_overrides"] = refreshed
            except Exception:
                pass
    doc_sources = doc_overrides.get("sources") if isinstance(doc_overrides, dict) else {}
    docs_primary = _bool(inputs.get("docs_primary"), default=True)
    if not apply_overrides:
        docs_primary = False

    def _manual_override(key: str) -> tuple[bool, Any]:
        if isinstance(manual_overrides, dict) and key in manual_overrides:
            return True, manual_overrides.get(key)
        return False, None

    def _doc_present(key: str) -> bool:
        if not isinstance(doc_sources, dict):
            return False
        for fact_type in DOC_FACT_SOURCE_KEYS.get(key, []):
            if doc_sources.get(fact_type):
                return True
        return False

    def _resolve_total_monthly(key: str, base: list[float], doc_total: float) -> list[float]:
        manual_active, manual_val = _manual_override(key)
        if apply_overrides and manual_active:
            source_map[key] = "manual"
            if isinstance(manual_val, list):
                return _clamp_month_list(manual_val)
            return [float(_float(manual_val)) / 12.0] * 12
        if apply_overrides and docs_primary and _doc_present(key):
            source_map[key] = "docs"
            return [float(doc_total) / 12.0] * 12
        source_map[key] = "investor"
        return base

    def _resolve_total_monthly_additive(key: str, base: list[float], doc_total: float) -> list[float]:
        manual_active, manual_val = _manual_override(key)
        if apply_overrides and manual_active:
            source_map[key] = "manual"
            if isinstance(manual_val, list):
                return _clamp_month_list(manual_val)
            return [float(_float(manual_val)) / 12.0] * 12
        if apply_overrides and docs_primary and _doc_present(key):
            base_total = sum(base)
            if base_total <= 0.0:
                source_map[key] = "docs"
                return [float(doc_total) / 12.0] * 12
            diff = abs(float(doc_total) - float(base_total))
            tol = max(1.0, 0.01 * max(float(doc_total), float(base_total)))
            if diff <= tol:
                source_map[key] = "docs"
                return [float(doc_total) / 12.0] * 12
            source_map[key] = "docs+investor"
            return [float((doc_total + base_total) / 12.0)] * 12
        source_map[key] = "investor"
        return base

    if isinstance(doc_overrides, dict):
        doc_w2 = _float(doc_overrides.get("w2_wages_total"))
        w2_wages = _resolve_total_monthly("w2_wages_total", w2_wages, doc_w2)
        doc_w2_wh = _float(doc_overrides.get("w2_withholding_total"))
        w2_withholding = _resolve_total_monthly("w2_withholding_total", w2_withholding, doc_w2_wh)

        doc_ira_dist = _float(doc_overrides.get("ira_distributions_total"))
        ira_distributions = _resolve_total_monthly("ira_distributions_total", ira_distributions, doc_ira_dist)
        doc_ira_withholding = _float(doc_overrides.get("ira_withholding_total"))
        ira_withholding = _resolve_total_monthly("ira_withholding_total", ira_withholding, doc_ira_withholding)
        doc_ira_gross = _float(doc_overrides.get("ira_distributions_gross_total"))
        if apply_overrides and docs_primary and _doc_present("ira_distributions_total"):
            if doc_ira_gross > 0:
                ira_distributions_gross = [doc_ira_gross / 12.0] * 12
                source_map["ira_distributions_gross_total"] = "docs"
            else:
                ira_distributions_gross = list(ira_distributions)
                source_map["ira_distributions_gross_total"] = "docs"
        else:
            ira_distributions_gross = [ira_distributions[i] + ira_withholding[i] for i in range(12)]

        doc_interest = _float(doc_overrides.get("interest_total"))
        interest_monthly = _resolve_total_monthly_additive("interest_total", interest_monthly, doc_interest)

        doc_div = _float(doc_overrides.get("dividends_ordinary_total"))
        dividends_monthly = _resolve_total_monthly("dividends_ordinary_total", dividends_monthly, doc_div)
        manual_qual_active, manual_qual_val = _manual_override("dividends_qualified_total")
        doc_qual = _float(doc_overrides.get("dividends_qualified_total"))
        resolved_div_total = sum(dividends_monthly)
        if apply_overrides and resolved_div_total > 0 and source_map.get("dividends_ordinary_total") in {"manual", "docs"}:
            if manual_qual_active:
                doc_qual_override = max(0.0, min(1.0, _float(manual_qual_val) / resolved_div_total))
                source_map["dividends_qualified_total"] = "manual"
            elif docs_primary and _doc_present("dividends_qualified_total") and doc_qual > 0:
                doc_qual_override = max(0.0, min(1.0, doc_qual / resolved_div_total))
                source_map["dividends_qualified_total"] = "docs"
            else:
                source_map["dividends_qualified_total"] = "investor"

        doc_cg = _float(doc_overrides.get("cap_gain_dist_total"))
        manual_cg_active, manual_cg_val = _manual_override("cap_gain_dist_total")
        if apply_overrides and manual_cg_active:
            lt_gains = [lt_gains[i] + (_float(manual_cg_val) / 12.0) for i in range(12)]
            source_map["cap_gain_dist_total"] = "manual"
        elif apply_overrides and docs_primary and _doc_present("cap_gain_dist_total"):
            lt_gains = [lt_gains[i] + (doc_cg / 12.0) for i in range(12)]
            source_map["cap_gain_dist_total"] = "docs"
        else:
            source_map["cap_gain_dist_total"] = "investor"

        doc_k1_total = (
            _float(doc_overrides.get("k1_ordinary_total"))
            + _float(doc_overrides.get("k1_interest_total"))
            + _float(doc_overrides.get("k1_dividends_total"))
            + _float(doc_overrides.get("k1_rental_total"))
            + _float(doc_overrides.get("k1_other_total"))
        )
        k1_monthly = _resolve_total_monthly("k1_total", k1_monthly, doc_k1_total)

        aca_premium_doc = _clamp_month_list(doc_overrides.get("aca_premium_monthly"))
        aca_premium = _resolve_total_monthly("aca_premium_monthly", aca_premium, sum(aca_premium_doc))
        if source_map.get("aca_premium_monthly") == "docs":
            aca_premium = aca_premium_doc

        aca_aptc_doc = _clamp_month_list(doc_overrides.get("aca_aptc_monthly"))
        aca_aptc = _resolve_total_monthly("aca_aptc_monthly", aca_aptc, sum(aca_aptc_doc))
        if source_map.get("aca_aptc_monthly") == "docs":
            aca_aptc = aca_aptc_doc

    estimated_payments_manual: list[dict[str, Any]] = []
    for row in inputs.get("estimated_payments") or []:
        if not isinstance(row, dict):
            continue
        date_raw = str(row.get("date") or "").strip()
        amt = _float(row.get("amount"))
        if not date_raw or amt == 0.0:
            continue
        try:
            d = dt.date.fromisoformat(date_raw[:10])
        except Exception:
            continue
        if d < start or d > end:
            continue
        estimated_payments_manual.append({"date": d, "amount": amt})

    est_payments_monthly = [0.0] * 12
    for row in estimated_payments_manual:
        idx = int(row["date"].month) - 1
        est_payments_monthly[idx] += abs(float(row["amount"]))

    est_payments_monthly = [est_payments_monthly[i] + estimated_payments_tagged[i] for i in range(12)]

    withholding_monthly = [
        ira_withholding[i] + w2_withholding[i] + w2_withholding_tagged[i] + other_withholding[i] for i in range(12)
    ]

    year_filing = (profile.filing_status or "MFJ").upper()
    filing_cfg = params.get("filing_status", {}).get(year_filing, {}) if isinstance(params, dict) else {}
    standard_deduction = _float(filing_cfg.get("standard_deduction") or 0.0)
    itemized_amount = _float(profile.itemized_amount or 0.0)
    deductions = standard_deduction if profile.deductions_mode == "standard" else itemized_amount

    qualified_dividend_pct = _float(inputs.get("qualified_dividend_pct"))
    if doc_qual_override is not None:
        qualified_dividend_pct = doc_qual_override
    niit_enabled = _bool(inputs.get("niit_enabled"), default=True)
    niit_rate = _float(inputs.get("niit_rate"))
    state_tax_rate = _float(inputs.get("state_tax_rate"))
    aca_enabled = _bool(inputs.get("aca_enabled"), default=True)

    monthly_rows: list[dict[str, Any]] = []
    totals = {
        "ordinary_income": 0.0,
        "st_gains": 0.0,
        "lt_gains": 0.0,
        "dividends": 0.0,
        "interest": 0.0,
        "ira_distributions": 0.0,
        "ira_distributions_net": 0.0,
        "ira_distributions_gross": 0.0,
        "yoga_net_profit": 0.0,
        "w2_wages": 0.0,
        "trust_passthrough": 0.0,
        "trust_passthrough_gross": 0.0,
        "trust_fees": 0.0,
        "withholding": 0.0,
        "estimated_payments": 0.0,
        "other_withholding": 0.0,
    }

    paid_ytd_by_month = []
    for idx in range(12):
        ytd_ira = sum(ira_distributions[: idx + 1])
        ytd_yoga = sum(yoga_net_profit[: idx + 1])
        ytd_w2 = sum(w2_wages[: idx + 1])
        ytd_trust = sum(trust_passthrough[: idx + 1])
        ytd_k1 = sum(k1_monthly[: idx + 1])
        ytd_interest = sum(interest_monthly[: idx + 1])
        ytd_div = sum(dividends_monthly[: idx + 1])
        ytd_st = sum(st_gains[: idx + 1])
        ytd_lt = sum(lt_gains[: idx + 1])

        ytd_ordinary = ytd_ira + ytd_yoga + ytd_w2 + ytd_trust + ytd_k1 + ytd_interest
        se_tax, se_deduction = compute_se_tax(ytd_yoga, params, year_filing)

        breakdown = compute_tax_breakdown(
            ordinary_core=ytd_ordinary,
            st_gains=ytd_st,
            lt_gains=ytd_lt,
            dividends=ytd_div,
            qualified_dividend_pct=qualified_dividend_pct,
            deductions=deductions,
            se_deduction=se_deduction,
            filing_status=year_filing,
            params=params,
            niit_enabled=niit_enabled,
            niit_rate=niit_rate,
        )
        income_tax = breakdown["ordinary_tax"] + breakdown["ltcg_tax"] + breakdown["niit_tax"]
        state_tax = max(0.0, (breakdown["ordinary_taxable"] + breakdown["ltcg_taxable"])) * state_tax_rate
        total_tax_ytd = income_tax + se_tax + state_tax

        paid_ytd = sum(withholding_monthly[: idx + 1]) + sum(est_payments_monthly[: idx + 1])
        paid_ytd_by_month.append(paid_ytd)
        trust_dividend_tax_ytd = sum(other_withholding[: idx + 1])

        run_rate_tax = total_tax_ytd / (idx + 1) * 12 if idx >= 0 else total_tax_ytd

        flags = []
        safe_harbor_target = _float(inputs.get("last_year_total_tax")) * _float(inputs.get("safe_harbor_multiplier") or 1.0)
        if safe_harbor_target > 0:
            prorated = safe_harbor_target * ((idx + 1) / 12.0)
            if paid_ytd + 1e-6 < prorated:
                flags.append("behind safe harbor")
        if paid_ytd + 1e-6 < total_tax_ytd * 0.9:
            flags.append("withholding shortfall")
        if abs(st_gains[idx] + lt_gains[idx]) >= 10000:
            flags.append("large cap gains month")

        monthly_rows.append(
            {
                "month": idx + 1,
                "label": MONTH_LABELS[idx],
                "ordinary_ytd": ytd_ordinary,
                "st_gains_ytd": ytd_st,
                "lt_gains_ytd": ytd_lt,
                "dividends_ytd": ytd_div,
                "tax_ytd": total_tax_ytd,
                "paid_ytd": paid_ytd,
                "trust_dividend_tax_ytd": trust_dividend_tax_ytd,
                "remaining_ytd": total_tax_ytd - paid_ytd,
                "run_rate_tax": run_rate_tax,
                "flags": flags,
            }
        )

    totals["ordinary_income"] = (
        sum(ira_distributions)
        + sum(yoga_net_profit)
        + sum(w2_wages)
        + sum(trust_passthrough)
        + sum(k1_monthly)
        + sum(interest_monthly)
    )
    totals["st_gains"] = sum(st_gains)
    totals["lt_gains"] = sum(lt_gains)
    totals["dividends"] = sum(dividends_monthly)
    totals["interest"] = sum(interest_monthly)
    totals["ira_distributions"] = sum(ira_distributions_gross)
    totals["ira_distributions_net"] = sum(ira_distributions)
    totals["yoga_net_profit"] = sum(yoga_net_profit)
    totals["w2_wages"] = sum(w2_wages)
    totals["trust_passthrough"] = sum(trust_passthrough)
    totals["trust_passthrough_gross"] = sum(trust_passthrough_gross)
    totals["trust_fees"] = sum(trust_fees)
    totals["k1_income"] = sum(k1_monthly)
    totals["withholding"] = sum(withholding_monthly)
    totals["estimated_payments"] = sum(est_payments_monthly)
    totals["other_withholding"] = sum(other_withholding)

    trust_pnl = trust_accounts_pnl(session, year=year) if include_trust else {"gross": 0.0}
    trust_pnl_gross = float(trust_pnl.get("gross") or 0.0)
    trust_pnl_net = max(0.0, trust_pnl_gross - totals["trust_fees"])

    pass_through_labels = _pass_through_labels(session, year=year)
    pass_through_div = 0.0
    pass_through_int = 0.0
    if pass_through_labels:
        div_rows = list_dividend_details(session, year=year)
        int_rows = list_interest_details(session, year=year)
        pass_through_div = sum(float(r.get("amount") or 0.0) for r in div_rows if r.get("account_name") in pass_through_labels)
        pass_through_int = sum(float(r.get("amount") or 0.0) for r in int_rows if r.get("account_name") in pass_through_labels)
        if pass_through_div > totals["dividends"]:
            pass_through_div = totals["dividends"]
        if pass_through_int > totals["interest"]:
            pass_through_int = totals["interest"]

    se_tax_total, se_deduction_total = compute_se_tax(totals["yoga_net_profit"], params, year_filing)
    breakdown_total = compute_tax_breakdown(
        ordinary_core=totals["ordinary_income"],
        st_gains=totals["st_gains"],
        lt_gains=totals["lt_gains"],
        dividends=totals["dividends"],
        qualified_dividend_pct=qualified_dividend_pct,
        deductions=deductions,
        se_deduction=se_deduction_total,
        filing_status=year_filing,
        params=params,
        niit_enabled=niit_enabled,
        niit_rate=niit_rate,
    )
    income_tax_total = breakdown_total["ordinary_tax"] + breakdown_total["ltcg_tax"] + breakdown_total["niit_tax"]
    state_tax_total = max(0.0, (breakdown_total["ordinary_taxable"] + breakdown_total["ltcg_taxable"])) * state_tax_rate
    total_tax = income_tax_total + se_tax_total + state_tax_total
    taxable_income_total = breakdown_total["ordinary_taxable"] + breakdown_total["ltcg_taxable"]
    gross_income_total = totals["ordinary_income"] + totals["dividends"] + totals["st_gains"] + totals["lt_gains"]
    effective_tax_rate = (total_tax / taxable_income_total) if taxable_income_total > 0 else 0.0
    effective_tax_rate_gross = (total_tax / gross_income_total) if gross_income_total > 0 else 0.0

    as_of_idx = min(12, max(1, int(as_of.month))) - 1
    paid_ytd = paid_ytd_by_month[as_of_idx] if paid_ytd_by_month else 0.0
    remaining_due = total_tax - paid_ytd

    safe_harbor_target = _float(inputs.get("last_year_total_tax")) * _float(inputs.get("safe_harbor_multiplier") or 1.0)
    safe_harbor_paid_target = safe_harbor_target * ((as_of_idx + 1) / 12.0) if safe_harbor_target > 0 else 0.0
    last_year_total_tax = _float(inputs.get("last_year_total_tax"))
    safe_harbor_multiplier = _float(inputs.get("safe_harbor_multiplier") or 1.0)

    fpl = _float((params.get("fpl") or {}).get("base")) + _float((params.get("fpl") or {}).get("per_additional")) * max(
        0, int(profile.household_size or 1) - 1
    )
    magi_est = totals["ordinary_income"] + totals["st_gains"] + totals["lt_gains"]
    magi_override = inputs.get("magi_override")
    if magi_override not in (None, ""):
        magi_est = _float(magi_override)
    fpl_ratio = magi_est / fpl if fpl > 0 else 0.0
    aca_indicator = "disabled"
    if aca_enabled:
        aca_indicator = "neutral"
        if fpl_ratio >= 4.0:
            aca_indicator = "likely payback"
        elif fpl_ratio > 0 and fpl_ratio <= 1.5:
            aca_indicator = "likely extra credit"

    ira_withholding_ytd = sum(ira_withholding[: as_of_idx + 1])
    w2_withholding_ytd = sum(w2_withholding[: as_of_idx + 1]) + sum(w2_withholding_tagged[: as_of_idx + 1])
    other_withholding_ytd = sum(other_withholding[: as_of_idx + 1])

    docs_present = isinstance(doc_sources, dict) and any(bool(v) for v in doc_sources.values())
    summary = {
        "year": year,
        "filing_status": year_filing,
        "state_tax_rate": state_tax_rate,
        "deductions_mode": profile.deductions_mode,
        "deductions": deductions,
        "standard_deduction": standard_deduction,
        "itemized_amount": itemized_amount,
        "ordinary_income": totals["ordinary_income"] + totals["dividends"],
        "ordinary_breakdown": {
            "ira_distributions": totals["ira_distributions"],
            "ira_distributions_net": totals["ira_distributions_net"],
            "w2_wages": totals["w2_wages"],
            "yoga_net_profit": totals["yoga_net_profit"],
            "trust_passthrough": totals["trust_passthrough"],
            "trust_passthrough_gross": totals["trust_passthrough_gross"],
            "trust_pnl_gross": trust_pnl_gross,
            "trust_pnl_net": trust_pnl_net,
            "trust_fees": totals["trust_fees"],
            "k1_income": totals.get("k1_income", 0.0),
            "interest": totals["interest"],
            "interest_pass_through": pass_through_int,
            "interest_other": max(0.0, totals["interest"] - pass_through_int),
            "dividends": totals["dividends"],
            "dividends_pass_through": pass_through_div,
            "dividends_other": max(0.0, totals["dividends"] - pass_through_div),
            "pass_through_income": pass_through_div + pass_through_int,
        },
        "capital_gains": {
            "st": totals["st_gains"],
            "lt": totals["lt_gains"],
        },
        "se_tax": se_tax_total,
        "se_deduction": se_deduction_total,
        "taxable_ordinary": breakdown_total["ordinary_taxable"],
        "taxable_ltcg": breakdown_total["ltcg_taxable"],
        "taxable_income": {
            "ordinary_taxable": breakdown_total["ordinary_taxable"],
            "ltcg_taxable": breakdown_total["ltcg_taxable"],
        },
        "ordinary_tax": breakdown_total["ordinary_tax"],
        "ltcg_tax": breakdown_total["ltcg_tax"],
        "niit_tax": breakdown_total["niit_tax"],
        "qualified_dividends": breakdown_total["qualified_dividends"],
        "non_qualified_dividends": breakdown_total["nonqualified_dividends"],
        "state_tax": state_tax_total,
        "total_tax": total_tax,
        "taxable_income_total": taxable_income_total,
        "gross_income_total": gross_income_total,
        "effective_tax_rate": effective_tax_rate,
        "effective_tax_rate_gross": effective_tax_rate_gross,
        "paid_ytd": paid_ytd,
        "remaining_due": remaining_due,
        "safe_harbor_target": safe_harbor_target,
        "last_year_total_tax": last_year_total_tax,
        "safe_harbor_multiplier": safe_harbor_multiplier,
        "safe_harbor_paid_target": safe_harbor_paid_target,
        "safe_harbor_paid_ytd": paid_ytd,
        "safe_harbor_status": "behind" if safe_harbor_target and paid_ytd < safe_harbor_paid_target else "on track",
        "withholding_ytd": sum(withholding_monthly[: as_of_idx + 1]),
        "ira_withholding_ytd": ira_withholding_ytd,
        "w2_withholding_ytd": w2_withholding_ytd,
        "other_withholding_ytd": other_withholding_ytd,
        "estimated_payments_ytd": sum(est_payments_monthly[: as_of_idx + 1]),
        "aca": {
            "enabled": aca_enabled,
            "premium_paid": sum(aca_premium),
            "aptc_received": sum(aca_aptc),
            "magi_estimate": magi_est,
            "fpl_ratio": fpl_ratio,
            "indicator": aca_indicator,
        },
        "credits": {
            "child_credit_est": _float((params.get("child_credit") or {}).get("per_child")) * int(profile.dependents_count or 0)
        },
        "sources": source_map,
        "docs_primary": docs_primary,
        "docs_present": docs_present,
    }

    tags = {
        "categories": TAX_TAG_CATEGORIES,
        "labels": TAX_TAG_LABELS,
    }

    return TaxDashboard(
        year=year,
        profile=profile,
        inputs=inputs,
        params=params,
        summary=summary,
        monthly=monthly_rows,
        tags=tags,
    )


def fetch_tax_tagged_transactions(
    session: Session,
    *,
    year: int,
    account_id: int | None = None,
    limit: int = 200,
) -> list[tuple[Transaction, Account, TaxTag | None]]:
    start, end = _year_bounds(year)
    q = (
        session.query(Transaction, Account, TaxTag)
        .join(Account, Account.id == Transaction.account_id)
        .outerjoin(TaxTag, TaxTag.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= end)
    )
    if account_id is not None:
        q = q.filter(Transaction.account_id == int(account_id))
    rows = q.order_by(Transaction.date.desc(), Transaction.id.desc()).limit(int(limit)).all()
    return rows


def list_tax_tagged_transactions(
    session: Session,
    *,
    year: int,
) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    rows = (
        session.query(Transaction, Account, TaxTag)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxTag, TaxTag.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= end)
        .order_by(Transaction.date.asc(), Transaction.id.asc())
        .all()
    )
    out: list[dict[str, Any]] = []
    for tx, acct, tag in rows:
        out.append(
            {
                "date": tx.date.isoformat(),
                "account_name": acct.name,
                "amount": float(tx.amount or 0.0),
                "category": tag.category if tag else "",
                "note": tag.note if tag else "",
                "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                "transaction_id": tx.id,
            }
        )
    return out


def list_dividend_details(session: Session, *, year: int) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    acct_ids = _account_ids_by_category(session)
    non_trust_ids = acct_ids.get("non_trust_non_ira") or []
    trust_ids = acct_ids.get("trust_non_ira") or []
    non_trust_income_ids = _income_event_account_ids(
        session,
        account_ids=non_trust_ids,
        start=start,
        end=end,
        types=("DIVIDEND",),
    )
    trust_begin = trust_start or start
    trust_income_ids = _income_event_account_ids(
        session,
        account_ids=trust_ids,
        start=trust_begin,
        end=end,
        types=("DIVIDEND",),
    )

    out: list[dict[str, Any]] = []

    if non_trust_ids:
        rows = (
            session.query(IncomeEvent, Account, TaxpayerEntity)
            .join(Account, Account.id == IncomeEvent.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                IncomeEvent.account_id.in_(non_trust_ids),
                IncomeEvent.date >= start,
                IncomeEvent.date <= end,
                IncomeEvent.type == "DIVIDEND",
            )
            .all()
        )
        for ev, acct, tp in rows:
            out.append(
                {
                    "date": ev.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": ev.type,
                    "amount": float(ev.amount or 0.0),
                    "description": ev.ticker or "",
                    "source": "IncomeEvent",
                }
            )

    if trust_ids:
        trust_begin = trust_start or start
        rows = (
            session.query(IncomeEvent, Account, TaxpayerEntity)
            .join(Account, Account.id == IncomeEvent.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                IncomeEvent.account_id.in_(trust_ids),
                IncomeEvent.date >= trust_begin,
                IncomeEvent.date <= end,
                IncomeEvent.type == "DIVIDEND",
            )
            .all()
        )
        for ev, acct, tp in rows:
            out.append(
                {
                    "date": ev.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": ev.type,
                    "amount": float(ev.amount or 0.0),
                    "description": ev.ticker or "",
                    "source": "IncomeEvent",
                }
            )

    if non_trust_ids:
        seen_tx: set[tuple[int, dt.date, float, str]] = set()
        q = (
            session.query(Transaction, Account, TaxpayerEntity)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                Transaction.account_id.in_(non_trust_ids),
                Transaction.date >= start,
                Transaction.date <= end,
                Transaction.type == "DIV",
            )
        )
        if non_trust_income_ids:
            q = q.filter(~Transaction.account_id.in_(list(non_trust_income_ids)))
        rows = q.all()
        for tx, acct, tp in rows:
            if _is_div_int_withholding_tx(tx):
                continue
            desc = (tx.lot_links_json or {}).get("description") or tx.ticker or ""
            key = (int(tx.account_id), tx.date, float(tx.amount or 0.0), desc)
            if key in seen_tx:
                continue
            seen_tx.add(key)
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": tx.type,
                    "amount": float(tx.amount or 0.0),
                    "description": desc,
                    "source": "Transaction",
                }
            )

    if trust_ids:
        seen_tx: set[tuple[int, dt.date, float, str]] = set()
        q = (
            session.query(Transaction, Account, TaxpayerEntity)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                Transaction.account_id.in_(trust_ids),
                Transaction.date >= trust_begin,
                Transaction.date <= end,
                Transaction.type == "DIV",
            )
        )
        if trust_income_ids:
            q = q.filter(~Transaction.account_id.in_(list(trust_income_ids)))
        rows = q.all()
        for tx, acct, tp in rows:
            if _is_div_int_withholding_tx(tx):
                continue
            desc = (tx.lot_links_json or {}).get("description") or tx.ticker or ""
            key = (int(tx.account_id), tx.date, float(tx.amount or 0.0), desc)
            if key in seen_tx:
                continue
            seen_tx.add(key)
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": tx.type,
                    "amount": float(tx.amount or 0.0),
                    "description": desc,
                    "source": "Transaction",
                }
            )

    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("description") or ""))
    return out


def dividend_summary_by_account(session: Session, *, year: int) -> list[dict[str, Any]]:
    rows = list_dividend_details(session, year=year)
    totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        acct = row.get("account_name") or "Unknown"
        totals.setdefault(
            acct,
            {
                "account_name": acct,
                "taxpayer": row.get("taxpayer") or "",
                "amount": 0.0,
                "count": 0,
            },
        )
        totals[acct]["amount"] += float(row.get("amount") or 0.0)
        totals[acct]["count"] += 1
    return sorted(totals.values(), key=lambda r: (r.get("amount") or 0.0) * -1)


def list_interest_details(session: Session, *, year: int) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    acct_ids = _account_ids_by_category(session)
    non_trust_ids = acct_ids.get("non_trust_non_ira") or []
    trust_ids = acct_ids.get("trust_non_ira") or []
    non_trust_income_ids = _income_event_account_ids(
        session,
        account_ids=non_trust_ids,
        start=start,
        end=end,
        types=("INTEREST",),
    )
    trust_begin = trust_start or start
    trust_income_ids = _income_event_account_ids(
        session,
        account_ids=trust_ids,
        start=trust_begin,
        end=end,
        types=("INTEREST",),
    )

    out: list[dict[str, Any]] = []
    doc_rows = (
        session.query(TaxFact, TaxDocument, HouseholdEntity)
        .join(TaxDocument, TaxDocument.id == TaxFact.source_doc_id)
        .outerjoin(HouseholdEntity, HouseholdEntity.id == TaxFact.owner_entity_id)
        .filter(
            TaxFact.tax_year == int(year),
            TaxFact.fact_type == "INT_INCOME",
            TaxFact.user_confirmed.is_(True),
        )
        .all()
    )

    def _doc_is_relevant(doc: TaxDocument) -> bool:
        if doc.status != "CONFIRMED":
            return False
        if doc.is_corrected and doc.is_authoritative is not True:
            return False
        if doc.is_authoritative is False:
            return False
        return True

    for fact, doc, owner in doc_rows:
        if not _doc_is_relevant(doc):
            continue
        owner_label = owner.display_name if owner else "Tax document"
        owner_type = str(owner.entity_type) if owner else ""
        out.append(
            {
                "date": (doc.uploaded_at or dt.datetime(year, 12, 31)).date(),
                "account_name": owner_label,
                "taxpayer": owner_type or owner_label,
                "type": "1099-INT",
                "amount": float(fact.amount or 0.0),
                "description": fact.payer_name or doc.filename,
                "source": "TaxDoc",
            }
        )

    if non_trust_ids:
        rows = (
            session.query(IncomeEvent, Account, TaxpayerEntity)
            .join(Account, Account.id == IncomeEvent.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                IncomeEvent.account_id.in_(non_trust_ids),
                IncomeEvent.date >= start,
                IncomeEvent.date <= end,
                IncomeEvent.type == "INTEREST",
            )
            .all()
        )
        for ev, acct, tp in rows:
            out.append(
                {
                    "date": ev.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": ev.type,
                    "amount": float(ev.amount or 0.0),
                    "description": ev.ticker or "",
                    "source": "IncomeEvent",
                }
            )

    if trust_ids:
        trust_begin = trust_start or start
        rows = (
            session.query(IncomeEvent, Account, TaxpayerEntity)
            .join(Account, Account.id == IncomeEvent.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                IncomeEvent.account_id.in_(trust_ids),
                IncomeEvent.date >= trust_begin,
                IncomeEvent.date <= end,
                IncomeEvent.type == "INTEREST",
            )
            .all()
        )
        for ev, acct, tp in rows:
            out.append(
                {
                    "date": ev.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": ev.type,
                    "amount": float(ev.amount or 0.0),
                    "description": ev.ticker or "",
                    "source": "IncomeEvent",
                }
            )

    if non_trust_ids:
        seen_tx: set[tuple[int, dt.date, float, str]] = set()
        q = (
            session.query(Transaction, Account, TaxpayerEntity)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                Transaction.account_id.in_(non_trust_ids),
                Transaction.date >= start,
                Transaction.date <= end,
                Transaction.type == "INT",
            )
        )
        if non_trust_income_ids:
            q = q.filter(~Transaction.account_id.in_(list(non_trust_income_ids)))
        rows = q.all()
        for tx, acct, tp in rows:
            if _is_div_int_withholding_tx(tx):
                continue
            desc = (tx.lot_links_json or {}).get("description") or tx.ticker or ""
            key = (int(tx.account_id), tx.date, float(tx.amount or 0.0), desc)
            if key in seen_tx:
                continue
            seen_tx.add(key)
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": tx.type,
                    "amount": float(tx.amount or 0.0),
                    "description": desc,
                    "source": "Transaction",
                }
            )

    if trust_ids:
        seen_tx: set[tuple[int, dt.date, float, str]] = set()
        q = (
            session.query(Transaction, Account, TaxpayerEntity)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                Transaction.account_id.in_(trust_ids),
                Transaction.date >= trust_begin,
                Transaction.date <= end,
                Transaction.type == "INT",
            )
        )
        if trust_income_ids:
            q = q.filter(~Transaction.account_id.in_(list(trust_income_ids)))
        rows = q.all()
        for tx, acct, tp in rows:
            if _is_div_int_withholding_tx(tx):
                continue
            desc = (tx.lot_links_json or {}).get("description") or tx.ticker or ""
            key = (int(tx.account_id), tx.date, float(tx.amount or 0.0), desc)
            if key in seen_tx:
                continue
            seen_tx.add(key)
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "type": tx.type,
                    "amount": float(tx.amount or 0.0),
                    "description": desc,
                    "source": "Transaction",
                }
            )

    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("description") or ""))
    return out


def list_ira_tax_details(session: Session, *, year: int, category: str) -> list[dict[str, Any]]:
    if category not in {"IRA_DISTRIBUTION", "IRA_WITHHOLDING"}:
        return []
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    rows = (
        session.query(Transaction, Account, TaxpayerEntity)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            Transaction.date >= start,
            Transaction.date <= end,
            Account.account_type == "IRA",
        )
        .all()
    )
    out: list[dict[str, Any]] = []
    for tx, acct, tp in rows:
        suggested = suggest_tax_tag(tx, acct, tp, trust_start=trust_start)
        if suggested != category:
            continue
        out.append(
            {
                "date": tx.date,
                "account_name": acct.name,
                "taxpayer": tp.name,
                "amount": abs(float(tx.amount or 0.0)),
                "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                "source": "Transaction",
            }
        )
    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("description") or ""))
    return out


def list_other_withholding_details(session: Session, *, year: int) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    acct_ids = _account_ids_by_category(session)
    non_trust_ids = acct_ids.get("non_trust_non_ira") or []
    trust_ids = acct_ids.get("trust_non_ira") or []

    out: list[dict[str, Any]] = []

    def _add_tx_rows(ids: list[int], min_date: dt.date) -> None:
        if not ids:
            return
        rows = (
            session.query(Transaction, Account, TaxpayerEntity)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                Transaction.account_id.in_(ids),
                Transaction.date >= min_date,
                Transaction.date <= end,
                Transaction.type == "WITHHOLDING",
            )
            .all()
        )
        for tx, acct, tp in rows:
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "amount": abs(float(tx.amount or 0.0)),
                    "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                    "source": "Transaction",
                }
            )

        div_int_rows = (
            session.query(Transaction, Account, TaxpayerEntity)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                Transaction.account_id.in_(ids),
                Transaction.date >= min_date,
                Transaction.date <= end,
                Transaction.type.in_(("DIV", "INT")),
                Transaction.amount < 0,
            )
            .all()
        )
        for tx, acct, tp in div_int_rows:
            if not _is_div_int_withholding_tx(tx):
                continue
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "amount": abs(float(tx.amount or 0.0)),
                    "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                    "source": "Transaction",
                }
            )

    def _add_income_rows(ids: list[int], min_date: dt.date) -> None:
        if not ids:
            return
        rows = (
            session.query(IncomeEvent, Account, TaxpayerEntity)
            .join(Account, Account.id == IncomeEvent.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(
                IncomeEvent.account_id.in_(ids),
                IncomeEvent.date >= min_date,
                IncomeEvent.date <= end,
                IncomeEvent.type == "WITHHOLDING",
            )
            .all()
        )
        for ev, acct, tp in rows:
            out.append(
                {
                    "date": ev.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "amount": abs(float(ev.amount or 0.0)),
                    "description": "",
                    "source": "IncomeEvent",
                }
            )

    _add_tx_rows(non_trust_ids, start)
    _add_income_rows(non_trust_ids, start)

    trust_begin = trust_start or start
    _add_tx_rows(trust_ids, trust_begin)
    _add_income_rows(trust_ids, trust_begin)

    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("description") or ""))
    return out


def list_w2_withholding_details(session: Session, *, year: int, inputs: dict[str, Any]) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    acct_ids = _account_ids_by_category(session)
    non_trust_ids = acct_ids.get("non_trust_non_ira") or []

    out: list[dict[str, Any]] = []
    try:
        from src.core.tax_documents import _doc_is_authoritative
    except Exception:
        _doc_is_authoritative = None

    rows = (
        session.query(TaxFact, TaxDocument, HouseholdEntity)
        .join(TaxDocument, TaxDocument.id == TaxFact.source_doc_id)
        .outerjoin(HouseholdEntity, HouseholdEntity.id == TaxFact.owner_entity_id)
        .filter(
            TaxFact.tax_year == int(year),
            TaxFact.fact_type == "FED_WITHHOLDING",
            TaxFact.user_confirmed.is_(True),
        )
        .all()
    )
    for fact, doc, owner in rows:
        if _doc_is_authoritative is not None and not _doc_is_authoritative(doc):
            continue
        doc_date = doc.uploaded_at.date() if getattr(doc, "uploaded_at", None) else start
        out.append(
            {
                "date": doc_date,
                "account_name": doc.filename or "W-2",
                "taxpayer": owner.display_name if owner else "",
                "amount": abs(float(fact.amount or 0.0)),
                "description": fact.payer_name or doc.filename or "W-2",
                "source": "Tax document",
            }
        )
    monthly = _clamp_month_list(inputs.get("daughter_w2_withholding_monthly"))
    for idx, amt in enumerate(monthly):
        if abs(amt) <= 0:
            continue
        out.append(
            {
                "date": dt.date(int(year), idx + 1, 1),
                "account_name": "",
                "taxpayer": "",
                "amount": abs(float(amt)),
                "description": "Manual input",
                "source": "Input",
            }
        )

    if non_trust_ids:
        rows = (
            session.query(Transaction, Account, TaxpayerEntity, TaxTag)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .join(TaxTag, TaxTag.transaction_id == Transaction.id)
            .filter(
                Transaction.account_id.in_(non_trust_ids),
                Transaction.date >= start,
                Transaction.date <= end,
                TaxTag.category == "W2_WITHHOLDING",
            )
            .all()
        )
        for tx, acct, tp, _tag in rows:
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "amount": abs(float(tx.amount or 0.0)),
                    "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                    "source": "Transaction",
                }
            )

    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("description") or ""))
    return out


def list_estimated_payment_details(session: Session, *, year: int, inputs: dict[str, Any]) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    acct_ids = _account_ids_by_category(session)
    non_trust_ids = acct_ids.get("non_trust_non_ira") or []
    trust_ids = acct_ids.get("trust_non_ira") or []

    out: list[dict[str, Any]] = []
    for row in inputs.get("estimated_payments") or []:
        if not isinstance(row, dict):
            continue
        date_raw = str(row.get("date") or "").strip()
        amt = _float(row.get("amount"))
        if not date_raw or amt == 0.0:
            continue
        try:
            d = dt.date.fromisoformat(date_raw[:10])
        except Exception:
            continue
        if d < start or d > end:
            continue
        out.append(
            {
                "date": d,
                "account_name": "",
                "taxpayer": "",
                "amount": abs(float(amt)),
                "description": "Manual input",
                "source": "Input",
            }
        )

    def _add_tagged_rows(ids: list[int], min_date: dt.date) -> None:
        if not ids:
            return
        rows = (
            session.query(Transaction, Account, TaxpayerEntity, TaxTag)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .join(TaxTag, TaxTag.transaction_id == Transaction.id)
            .filter(
                Transaction.account_id.in_(ids),
                Transaction.date >= min_date,
                Transaction.date <= end,
                TaxTag.category == "ESTIMATED_TAX_PAYMENT",
            )
            .all()
        )
        for tx, acct, tp, _tag in rows:
            out.append(
                {
                    "date": tx.date,
                    "account_name": acct.name,
                    "taxpayer": tp.name,
                    "amount": abs(float(tx.amount or 0.0)),
                    "description": (tx.lot_links_json or {}).get("description") or tx.ticker or "",
                    "source": "Transaction",
                }
            )

    _add_tagged_rows(non_trust_ids, start)
    trust_begin = trust_start or start
    _add_tagged_rows(trust_ids, trust_begin)

    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("description") or ""))
    return out


def interest_summary_by_account(session: Session, *, year: int) -> list[dict[str, Any]]:
    rows = list_interest_details(session, year=year)
    totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        acct = row.get("account_name") or "Unknown"
        totals.setdefault(
            acct,
            {
                "account_name": acct,
                "taxpayer": row.get("taxpayer") or "",
                "amount": 0.0,
                "count": 0,
                "_doc_amount": 0.0,
                "_doc_count": 0,
                "_base_amount": 0.0,
                "_base_count": 0,
            },
        )
        amt = float(row.get("amount") or 0.0)
        if row.get("source") == "TaxDoc":
            totals[acct]["_doc_amount"] += amt
            totals[acct]["_doc_count"] += 1
        else:
            totals[acct]["_base_amount"] += amt
            totals[acct]["_base_count"] += 1
        totals[acct]["amount"] += amt
        totals[acct]["count"] += 1

    for entry in totals.values():
        if entry.get("_doc_count"):
            entry["amount"] = float(entry.get("_doc_amount") or 0.0)
            entry["count"] = int(entry.get("_doc_count") or 0)
    return sorted(totals.values(), key=lambda r: (r.get("amount") or 0.0) * -1)


def list_capital_gains_details(session: Session, *, year: int) -> list[dict[str, Any]]:
    from src.core.broker_tax import broker_realized_gains

    _summary, _by_symbol, detail_rows, _coverage = broker_realized_gains(session, scope="household", year=year)
    out: list[dict[str, Any]] = []
    for row in detail_rows:
        out.append(
            {
                "date": row.trade_date,
                "account_name": row.account_name or row.provider_account_id,
                "provider_account_id": row.provider_account_id,
                "symbol": row.symbol,
                "quantity": row.quantity_closed,
                "proceeds": row.proceeds,
                "basis": row.basis,
                "realized": row.realized,
                "term": row.term,
            }
        )
    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("symbol") or ""))
    return out


def symbol_summary_totals_by_account(session: Session, *, year: int) -> dict[str, float]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    conn_ids = _connection_ids_for_household(session, include_trust=True, start=start, end=end)
    if not conn_ids:
        return {}

    maps = (
        session.query(ExternalAccountMap, Account, TaxpayerEntity)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalAccountMap.connection_id.in_(conn_ids))
        .all()
    )
    acct_lookup: dict[tuple[int, str], dict[str, Any]] = {}
    for m, acct, tp in maps:
        acct_lookup[(int(m.connection_id), str(m.provider_account_id or "").strip())] = {
            "account_name": acct.name,
            "taxpayer_type": str(tp.type or "").upper(),
        }

    rows = (
        session.query(BrokerSymbolSummary)
        .filter(
            BrokerSymbolSummary.connection_id.in_(conn_ids),
            BrokerSymbolSummary.as_of_date >= start,
            BrokerSymbolSummary.as_of_date <= end,
        )
        .all()
    )
    totals: dict[str, float] = {}
    for row in rows:
        acct_key = (int(row.connection_id), str(row.provider_account_id or "").strip())
        acct_info = acct_lookup.get(acct_key)
        if acct_info:
            if trust_start and acct_info.get("taxpayer_type") == "TRUST" and row.as_of_date < trust_start:
                continue
            acct_name = acct_info.get("account_name") or row.provider_account_id
        else:
            acct_name = row.provider_account_id or "Unknown"
        totals[acct_name] = float(totals.get(acct_name) or 0.0) + float(row.realized_pl or 0.0)
    return totals


def capital_gains_summary_by_account(session: Session, *, year: int) -> list[dict[str, Any]]:
    rows = list_capital_gains_details(session, year=year)
    acct_names = {row.get("account_name") for row in rows if row.get("account_name")}
    taxpayer_by_acct: dict[str, str] = {}
    if acct_names:
        acct_rows = (
            session.query(Account, TaxpayerEntity)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(Account.name.in_(acct_names))
            .all()
        )
        for acct, tp in acct_rows:
            taxpayer_by_acct[acct.name] = tp.name

    wash_totals = wash_sale_summary_by_account(session, year=year)
    totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        acct_name = row.get("account_name") or row.get("provider_account_id") or "Unknown"
        term = str(row.get("term") or "").upper()
        realized = float(row.get("realized") or 0.0)
        entry = totals.setdefault(
            acct_name,
            {
                "account_name": acct_name,
                "taxpayer": taxpayer_by_acct.get(acct_name, ""),
                "st_realized": 0.0,
                "lt_realized": 0.0,
                "unknown_realized": 0.0,
                "total_realized": 0.0,
                "wash_adjustment": 0.0,
                "realized_with_wash": 0.0,
                "count": 0,
            },
        )
        if term == "LT":
            entry["lt_realized"] += realized
        elif term == "ST":
            entry["st_realized"] += realized
        else:
            entry["unknown_realized"] += realized
        entry["total_realized"] += realized
        entry["count"] += 1
    for acct, amt in wash_totals.items():
        entry = totals.setdefault(
            acct,
            {
                "account_name": acct,
                "taxpayer": taxpayer_by_acct.get(acct, ""),
                "st_realized": 0.0,
                "lt_realized": 0.0,
                "unknown_realized": 0.0,
                "total_realized": 0.0,
                "wash_adjustment": 0.0,
                "realized_with_wash": 0.0,
                "count": 0,
            },
        )
        entry["wash_adjustment"] += float(amt or 0.0)

    symbol_summary_totals = symbol_summary_totals_by_account(session, year=year)
    for acct, realized_total in symbol_summary_totals.items():
        entry = totals.setdefault(
            acct,
            {
                "account_name": acct,
                "taxpayer": taxpayer_by_acct.get(acct, ""),
                "st_realized": 0.0,
                "lt_realized": 0.0,
                "unknown_realized": 0.0,
                "total_realized": 0.0,
                "wash_adjustment": 0.0,
                "realized_with_wash": 0.0,
                "count": 0,
            },
        )
        entry["realized_with_wash"] = float(realized_total or 0.0)
        entry["wash_adjustment"] = float(realized_total or 0.0) - float(entry.get("total_realized") or 0.0)
        entry["symbol_summary_used"] = True

    for entry in totals.values():
        if entry.get("symbol_summary_used"):
            continue
        entry["realized_with_wash"] = float(entry.get("total_realized") or 0.0) + float(entry.get("wash_adjustment") or 0.0)

    return sorted(totals.values(), key=lambda r: r.get("realized_with_wash") or 0.0, reverse=True)


def trust_accounts_pnl(session: Session, *, year: int) -> dict[str, float]:
    pass_through_labels = _pass_through_labels(session, year=year)
    if not pass_through_labels:
        return {"gross": 0.0, "dividends": 0.0, "interest": 0.0, "capital_gains": 0.0}

    cap_by_acct = capital_gains_summary_by_account(session, year=year)
    trust_cap = sum(
        float(row.get("realized_with_wash") or 0.0) for row in cap_by_acct if row.get("account_name") in pass_through_labels
    )

    div_rows = list_dividend_details(session, year=year)
    trust_div = sum(float(r.get("amount") or 0.0) for r in div_rows if r.get("account_name") in pass_through_labels)

    int_rows = list_interest_details(session, year=year)
    trust_int = sum(float(r.get("amount") or 0.0) for r in int_rows if r.get("account_name") in pass_through_labels)

    return {
        "gross": trust_cap + trust_div + trust_int,
        "dividends": trust_div,
        "interest": trust_int,
        "capital_gains": trust_cap,
    }


def list_trust_pnl_details(session: Session, *, year: int) -> list[dict[str, Any]]:
    pass_through_labels = _pass_through_labels(session, year=year)
    acct_rows = (
        session.query(Account, TaxpayerEntity)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(Account.account_type != "IRA")
        .all()
    )
    if not acct_rows:
        return []

    trust_account_names = {acct.name for acct, _tp in acct_rows if acct.name in pass_through_labels}
    taxpayer_by_acct = {acct.name: tp.name for acct, tp in acct_rows if acct.name in pass_through_labels}
    if not trust_account_names:
        return []

    totals: dict[str, dict[str, Any]] = {}
    for acct_name in trust_account_names:
        totals[acct_name] = {
            "account_name": acct_name,
            "taxpayer": taxpayer_by_acct.get(acct_name, ""),
            "capital_gains": 0.0,
            "dividends": 0.0,
            "interest": 0.0,
            "total": 0.0,
        }

    cap_summary = capital_gains_summary_by_account(session, year=year)
    for row in cap_summary:
        acct_name = row.get("account_name")
        if acct_name in totals:
            totals[acct_name]["capital_gains"] = float(row.get("realized_with_wash") or 0.0)

    div_summary = dividend_summary_by_account(session, year=year)
    for row in div_summary:
        acct_name = row.get("account_name")
        if acct_name in totals:
            totals[acct_name]["dividends"] = float(row.get("amount") or 0.0)

    int_summary = interest_summary_by_account(session, year=year)
    for row in int_summary:
        acct_name = row.get("account_name")
        if acct_name in totals:
            totals[acct_name]["interest"] = float(row.get("amount") or 0.0)

    rows: list[dict[str, Any]] = []
    for entry in totals.values():
        entry["total"] = float(entry.get("capital_gains") or 0.0) + float(entry.get("dividends") or 0.0) + float(
            entry.get("interest") or 0.0
        )
        rows.append(entry)

    rows.sort(key=lambda r: r.get("total") or 0.0, reverse=True)
    return rows


def list_wash_sale_details(session: Session, *, year: int) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)
    conn_ids = _connection_ids_for_household(session, include_trust=True, start=start, end=end)
    maps = (
        session.query(ExternalAccountMap, Account, TaxpayerEntity)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalAccountMap.connection_id.in_(conn_ids))
        .all()
    )
    acct_lookup: dict[tuple[int, str], dict[str, Any]] = {}
    for m, acct, tp in maps:
        key = (int(m.connection_id), str(m.provider_account_id or "").strip())
        acct_lookup[key] = {
            "account_name": acct.name,
            "taxpayer": tp.name,
            "taxpayer_type": str(tp.type or "").upper(),
        }

    rows = (
        session.query(BrokerWashSaleEvent)
        .filter(
            BrokerWashSaleEvent.connection_id.in_(conn_ids),
            BrokerWashSaleEvent.trade_date >= start,
            BrokerWashSaleEvent.trade_date <= end,
        )
        .all()
    )
    out: list[dict[str, Any]] = []
    seen: set[tuple[int, str, int]] = set()
    seen_hashes: set[str] = set()
    for w in rows:
        raw = w.raw_json or {}
        src_row = raw.get("source_row")
        if src_row is not None:
            try:
                src_row_i = int(src_row)
            except Exception:
                src_row_i = None
            if src_row_i is not None:
                key = (int(w.connection_id), str(w.source_file_hash), src_row_i)
                if key in seen:
                    continue
                seen.add(key)

        acct_key = (int(w.connection_id), str(w.provider_account_id or "").strip())
        acct_info = acct_lookup.get(acct_key)
        if acct_info:
            if trust_start and acct_info.get("taxpayer_type") == "TRUST" and w.trade_date < trust_start:
                continue
        raw_row = raw.get("row") or {}
        if raw_row:
            try:
                payload = json.dumps(raw_row, sort_keys=True)
            except Exception:
                payload = None
            if payload is not None:
                row_hash = hashlib.sha1(payload.encode("utf-8")).hexdigest()
                if row_hash in seen_hashes:
                    continue
                seen_hashes.add(row_hash)
        amt = raw_row.get("FifoPnlRealized")
        try:
            realized = float(amt)
        except Exception:
            realized = float(w.realized_pl_fifo or 0.0)

        out.append(
            {
                "date": w.trade_date,
                "account_name": (acct_info or {}).get("account_name") or w.provider_account_id,
                "taxpayer": (acct_info or {}).get("taxpayer") or "",
                "symbol": w.symbol,
                "quantity": float(w.quantity or 0.0),
                "realized": realized,
                "source": "Broker",
            }
        )
    out.sort(key=lambda r: (r.get("date") or dt.date.min, r.get("account_name") or "", r.get("symbol") or ""))
    return out


def wash_sale_summary_by_account(session: Session, *, year: int) -> dict[str, float]:
    rows = list_wash_sale_details(session, year=year)
    totals: dict[str, float] = {}
    for row in rows:
        acct = row.get("account_name") or "Unknown"
        totals[acct] = float(totals.get(acct) or 0.0) + float(row.get("realized") or 0.0)
    return totals


def tax_account_summaries(session: Session, *, year: int) -> list[dict[str, Any]]:
    start, end = _year_bounds(year)
    trust_start = _trust_start_for_year(year)

    rows = (
        session.query(Account, TaxpayerEntity)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .order_by(Account.name.asc())
        .all()
    )
    summaries: dict[int, dict[str, Any]] = {}
    for acct, tp in rows:
        summaries[acct.id] = {
            "account_id": acct.id,
            "account_name": acct.name,
            "account_type": acct.account_type,
            "taxpayer": tp.name,
            "taxpayer_type": tp.type,
            "ira_distributions": 0.0,
            "ira_withholding": 0.0,
            "trust_distributions": 0.0,
            "other_withholding": 0.0,
        }

    tagged_rows = (
        session.query(Transaction, Account, TaxpayerEntity, TaxTag)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .join(TaxTag, TaxTag.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= end)
        .all()
    )
    for tx, acct, tp, tag in tagged_rows:
        if trust_start and str(tp.type or "").upper() == "TRUST" and tx.date < trust_start:
            continue
        row = summaries.get(acct.id)
        if row is None:
            continue
        amt = abs(float(tx.amount or 0.0))
        cat = tag.category
        if str(acct.account_type or "").upper() == "IRA":
            suggested = suggest_tax_tag(tx, acct, tp, trust_start=trust_start)
            if suggested is None:
                continue
            cat = suggested
        if cat == "IRA_DISTRIBUTION":
            row["ira_distributions"] += amt
        elif cat == "IRA_WITHHOLDING":
            row["ira_withholding"] += amt
        elif cat == "TRUST_DISTRIBUTION":
            row["trust_distributions"] += amt

    withholding_rows = (
        session.query(Transaction, Account, TaxpayerEntity)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            Transaction.date >= start,
            Transaction.date <= end,
            Transaction.type == "WITHHOLDING",
        )
        .all()
    )
    for tx, acct, tp in withholding_rows:
        if str(acct.account_type or "").upper() == "IRA":
            continue
        if trust_start and str(tp.type or "").upper() == "TRUST" and tx.date < trust_start:
            continue
        row = summaries.get(acct.id)
        if row is None:
            continue
        row["other_withholding"] += abs(float(tx.amount or 0.0))

    income_rows = (
        session.query(IncomeEvent, Account, TaxpayerEntity)
        .join(Account, Account.id == IncomeEvent.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            IncomeEvent.date >= start,
            IncomeEvent.date <= end,
            IncomeEvent.type == "WITHHOLDING",
        )
        .all()
    )
    for ev, acct, tp in income_rows:
        if str(acct.account_type or "").upper() == "IRA":
            continue
        if trust_start and str(tp.type or "").upper() == "TRUST" and ev.date < trust_start:
            continue
        row = summaries.get(acct.id)
        if row is None:
            continue
        row["other_withholding"] += abs(float(ev.amount or 0.0))

    out = []
    for row in summaries.values():
        if str(row.get("account_type") or "").upper() == "IRA":
            row["ira_distributions_net"] = float(row.get("ira_distributions") or 0.0)
            row["ira_distributions_gross"] = float(row.get("ira_distributions") or 0.0) + float(row.get("ira_withholding") or 0.0)
        else:
            row["ira_distributions_gross"] = float(row.get("ira_distributions") or 0.0)
        has_activity = any(
            float(row.get(k) or 0.0) > 0.0
            for k in ("ira_distributions", "ira_withholding", "trust_distributions", "other_withholding")
        )
        if not has_activity:
            continue
        out.append(row)
    out.sort(key=lambda r: (str(r.get("taxpayer_type") or ""), str(r.get("account_name") or "")))
    return out
