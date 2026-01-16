from __future__ import annotations

import datetime as dt
import csv
import io
from pathlib import Path
from decimal import Decimal

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse, Response
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.db.models import Account, ExpenseAccount, ExpenseAccountBalance, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.categorize import apply_rules_to_db, load_rules, write_starter_rules
from src.investor.expenses.categories import ensure_category, list_categories
from src.investor.expenses.config import load_expenses_config
from src.investor.expenses.db import ImportOptions, import_csv_statement_text, sha256_bytes
from src.investor.expenses.learning import learn_unknown_merchant_category
from src.investor.expenses.merchant_category import set_merchant_category
from src.investor.expenses.merchant_settings import get_merchant_setting, merchant_key, upsert_merchant_setting
from src.investor.expenses.importers.base import read_csv_rows
from src.investor.expenses.normalize import extract_last4_digits, money_2dp, normalize_bank_merchant, normalize_merchant
from src.investor.expenses.purge import purge_account_data, purge_all_expenses_data
from src.investor.expenses.rj_cash_out import import_rj_cash_outs
from src.investor.expenses.recurring import detect_recurring
from src.investor.expenses.reports import (
    budget_vs_actual,
    cardholders_by_spend,
    category_summary,
    opportunities,
    merchants_by_spend,
)
from src.utils.time import ensure_utc, utcfromtimestamp, utcnow


router = APIRouter(prefix="/expenses", tags=["expenses"])


def _safe_return_to(value: str, *, default: str) -> str:
    s = (value or "").strip()
    if not s:
        return default
    # Avoid open redirects; keep navigation internal.
    if "://" in s or not s.startswith("/"):
        return default
    return s


def _infer_account_label(*, institution: str, last4: str | None) -> str:
    inst = (institution or "").strip() or "Unknown"
    if last4:
        return f"{inst} ****{last4}"
    return f"{inst} (unnamed)"


def _infer_import_labels(
    *,
    content: str,
    institution: str,
    account: str,
    account_last4: str | None,
) -> tuple[str, str, str | None]:
    inst = institution.strip() or "AMEX"
    acct = account.strip()
    last4 = account_last4.strip() if account_last4 else None
    if last4:
        last4 = extract_last4_digits(last4)
    if not acct or not last4:
        try:
            _headers, rows = read_csv_rows(content)
            first = rows[0] if rows else {}
            if not last4:
                for key in ["Account #", "Account Number", "Account", "Acct #", "Acct"]:
                    if first.get(key):
                        last4 = extract_last4_digits(first.get(key, ""))
                        if last4:
                            break
            if not acct:
                acct = _infer_account_label(institution=inst, last4=last4)
        except Exception:
            if not acct:
                acct = _infer_account_label(institution=inst, last4=last4)
    return inst, acct, last4


def _parse_int(raw: str) -> int | None:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _account_ids_from_param(session: Session, raw: str) -> tuple[str, list[int] | None]:
    """
    Parses the account selector used by report views.

    Supported values:
    - "0" / "" -> all accounts
    - "credit" -> all CREDIT expense accounts
    - "bank"   -> all BANK expense accounts
    - "<int>"  -> specific expense_account_id
    """
    s = (raw or "").strip()
    if not s or s == "0":
        return "0", None
    lowered = s.lower()
    if lowered in {"credit", "cards", "credit_cards"}:
        ids = [r[0] for r in session.query(ExpenseAccount.id).filter(ExpenseAccount.type == "CREDIT").all()]
        return "credit", ids
    if lowered in {"bank", "banks"}:
        ids = [r[0] for r in session.query(ExpenseAccount.id).filter(ExpenseAccount.type == "BANK").all()]
        return "bank", ids
    i = _parse_int(s)
    if i and i > 0:
        return str(i), [int(i)]
    return "0", None


def _month_bounds(year: int, month: int) -> tuple[dt.date, dt.date]:
    start = dt.date(year, month, 1)
    end = dt.date(year, month, 28) + dt.timedelta(days=4)
    end = end.replace(day=1) - dt.timedelta(days=1)
    return start, end


@router.get("")
def expenses_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    cfg, cfg_path = load_expenses_config()
    error = request.query_params.get("error")
    ok = request.query_params.get("ok")
    accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()
    balances = (
        session.query(ExpenseAccountBalance)
        .order_by(ExpenseAccountBalance.as_of_date.desc(), ExpenseAccountBalance.id.desc())
        .all()
    )
    balances_by_account: dict[int, ExpenseAccountBalance] = {}
    for b in balances:
        if b.expense_account_id not in balances_by_account:
            balances_by_account[b.expense_account_id] = b
    batches = session.query(ExpenseImportBatch).order_by(ExpenseImportBatch.imported_at.desc()).limit(25).all()
    txn_count = int(session.query(func.count(ExpenseTransaction.id)).scalar() or 0)
    rj_accounts = session.query(Account).filter(Account.broker == "RJ").order_by(Account.name.asc()).all()
    from src.app.main import templates

    rp = Path(cfg.categorization.rules_path)
    rules_exists = rp.exists()
    rules_mtime = utcfromtimestamp(rp.stat().st_mtime) if rules_exists else None

    now_utc = utcnow()

    def _relative_time(ts: dt.datetime | None) -> str:
        if ts is None:
            return "—"
        try:
            delta = now_utc - ensure_utc(ts)
        except Exception:
            return "—"
        seconds = max(0, int(delta.total_seconds()))
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            return f"{seconds // 60}m ago"
        if seconds < 86400:
            return f"{seconds // 3600}h ago"
        days = seconds // 86400
        if days < 7:
            return f"{days}d ago"
        try:
            m = ensure_utc(ts).strftime("%b")
            return f"{m} {int(ensure_utc(ts).day)}"
        except Exception:
            return ensure_utc(ts).date().isoformat()

    last_batch = batches[0] if batches else None
    last_import_rel = _relative_time(last_batch.imported_at if last_batch else None)
    last_import_rows = int(getattr(last_batch, "row_count", 0) or 0) if last_batch else 0
    last_import_dupes = int(getattr(last_batch, "duplicates_skipped", 0) or 0) if last_batch else 0

    batch_rel_by_id: dict[int, str] = {}
    for b in batches:
        try:
            batch_rel_by_id[int(b.id)] = _relative_time(b.imported_at)
        except Exception:
            continue

    # Avoid global banner on this page; show compact inline alert instead.
    auth_banner_detail = auth_banner_message()

    # Bust CSS/JS cache for this page during development.
    static_version: str = "0"
    try:
        static_dir = Path(__file__).resolve().parents[1] / "static"
        css_mtime = int((static_dir / "app.css").stat().st_mtime)
        js_mtime = int((static_dir / "expenses_home.js").stat().st_mtime) if (static_dir / "expenses_home.js").exists() else 0
        static_version = str(max(css_mtime, js_mtime))
    except Exception:
        static_version = "0"

    return templates.TemplateResponse(
        "expenses_home.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_detail,
            "static_version": static_version,
            "error": error,
            "ok": ok,
            "expenses_active_tab": "statements",
            "expenses_badge": "Statements",
            "expenses_nav_year": dt.date.today().strftime("%Y"),
            "expenses_txn_count": txn_count,
            "show_rules_callout": True,
            "cfg_path": cfg_path,
            "enabled_formats": cfg.provider_formats,
            "accounts": accounts,
            "account_balances": balances_by_account,
            "batches": batches,
            "batch_rel_by_id": batch_rel_by_id,
            "txn_count": txn_count,
            "rj_accounts": rj_accounts,
            "rules_name": rp.name,
            "rules_path": str(rp),
            "rules_exists": rules_exists,
            "rules_mtime": rules_mtime,
            "last_import_rel": last_import_rel,
            "last_import_rows": last_import_rows,
            "last_import_dupes": last_import_dupes,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/import")
async def expenses_import(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    institution: str = Form(default=""),
    account: str = Form(default=""),
    account_type: str = Form(default="CREDIT"),
    account_last4: str = Form(default=""),
    cardholder_name: str = Form(default=""),
    format: str = Form(default=""),
    fuzzy_dedupe: str = Form(default="on"),
    store_original_rows: str = Form(default=""),
    file: UploadFile = File(...),
):
    cfg, _ = load_expenses_config()
    content_b = await file.read()
    file_hash = sha256_bytes(content_b)
    content = content_b.decode("utf-8-sig", errors="ignore")
    opts = ImportOptions(
        format_name=(format.strip() or None),
        fuzzy_dedupe=(fuzzy_dedupe == "on"),
        store_original_rows=(store_original_rows == "on"),
    )
    file_name = file.filename or "upload.csv"

    inst, acct, last4 = _infer_import_labels(
        content=content,
        institution=institution,
        account=account,
        account_last4=(account_last4.strip() or None),
    )

    try:
        res = import_csv_statement_text(
            session=session,
            cfg=cfg,
            content=content,
            file_name=file_name,
            file_hash=file_hash,
            institution=inst,
            account_name=acct,
            account_type=account_type.strip().upper(),
            account_last4=last4,
            default_cardholder_name=(cardholder_name.strip() or None),
            options=opts,
        )
        # Auto-categorize after import so reports aren't all "Unknown".
        apply_rules_to_db(
            session=session,
            rules_path=Path(cfg.categorization.rules_path),
            config=cfg.categorization,
            rebuild=False,
        )
    except Exception as e:
        return RedirectResponse(url=f"/expenses?error={type(e).__name__}: {e}", status_code=303)
    return RedirectResponse(
        url=f"/expenses?ok=Imported%20{res.inserted}%20txns%20(%2Bskipped%20{res.duplicates_skipped}%20dupes)",
        status_code=303,
    )


@router.post("/import-batch")
async def expenses_import_batch(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    institution: str = Form(default=""),
    account: str = Form(default=""),
    account_type: str = Form(default="CREDIT"),
    account_last4: str = Form(default=""),
    cardholder_name: str = Form(default=""),
    format: str = Form(default=""),
    fuzzy_dedupe: str = Form(default="on"),
    store_original_rows: str = Form(default=""),
    files: list[UploadFile] | None = File(default=None),
    dir_files: list[UploadFile] | None = File(default=None),
):
    cfg, cfg_path = load_expenses_config()
    opts = ImportOptions(
        format_name=(format.strip() or None),
        fuzzy_dedupe=(fuzzy_dedupe == "on"),
        store_original_rows=(store_original_rows == "on"),
    )

    all_files: list[UploadFile] = []
    if files:
        all_files.extend(files)
    if dir_files:
        all_files.extend(dir_files)
    if not all_files:
        return RedirectResponse(url="/expenses?error=No%20files%20selected", status_code=303)

    results: list[dict] = []
    for f in all_files:
        name = f.filename or "upload.csv"
        ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
        if ext not in {"csv", "tsv"}:
            results.append({"file_name": name, "status": "skipped", "error": "Not a .csv/.tsv file"})
            continue
        content_b = await f.read()
        file_hash = sha256_bytes(content_b)
        content = content_b.decode("utf-8-sig", errors="ignore")
        inst, acct, last4 = _infer_import_labels(
            content=content,
            institution=institution,
            account=account,
            account_last4=(account_last4.strip() or None),
        )
        try:
            res = import_csv_statement_text(
                session=session,
                cfg=cfg,
                content=content,
                file_name=name,
                file_hash=file_hash,
                institution=inst,
                account_name=acct,
                account_type=account_type.strip().upper(),
                account_last4=last4,
                default_cardholder_name=(cardholder_name.strip() or None),
                options=opts,
            )
            results.append({"file_name": name, "status": "ok", "result": res.model_dump()})
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            if "File already imported" in msg:
                results.append({"file_name": name, "status": "skipped", "error": msg})
            else:
                results.append({"file_name": name, "status": "error", "error": msg})

    # Categorize once at the end (faster for large batches).
    apply_rules_to_db(
        session=session,
        rules_path=Path(cfg.categorization.rules_path),
        config=cfg.categorization,
        rebuild=False,
    )

    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_import_results.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "cfg_path": cfg_path,
            "results": results,
        },
    )


@router.post("/import-rj-cash-outs")
def expenses_import_rj_cash_outs(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    rj_account_id: str = Form(default=""),
    expense_account_name: str = Form(default="Kolozsi Trust"),
    year: str = Form(default=""),
):
    cfg, _ = load_expenses_config()
    y = _parse_int(year) or 0
    start = dt.date(y, 1, 1) if y else None
    end = dt.date(y, 12, 31) if y else None

    aid = _parse_int(rj_account_id) or 0
    if not aid:
        row = session.query(Account).filter(Account.broker == "RJ").order_by(Account.id.asc()).first()
        if row is None:
            return RedirectResponse(url="/expenses?error=No%20RJ%20brokerage%20account%20found", status_code=303)
        aid = int(row.id)

    try:
        import re

        redaction_pats = [re.compile(p) for p in cfg.redaction.patterns] if cfg.redaction.enabled else []
        res = import_rj_cash_outs(
            session=session,
            rj_account_id=aid,
            expense_account_name=(expense_account_name.strip() or "Kolozsi Trust"),
            start_date=start,
            end_date=end,
            redaction_enabled=cfg.redaction.enabled,
            redaction_patterns=redaction_pats,
        )
        # Auto-categorize so reports aren't all "Unknown".
        apply_rules_to_db(
            session=session,
            rules_path=Path(cfg.categorization.rules_path),
            config=cfg.categorization,
            rebuild=False,
        )
    except Exception as e:
        return RedirectResponse(url=f"/expenses?error={type(e).__name__}: {e}", status_code=303)

    return RedirectResponse(
        url=f"/expenses?ok=Imported%20{res.inserted}%20RJ%20cash-out%20transactions%20(%2Bskipped%20{res.duplicates_skipped}%20dupes)",
        status_code=303,
    )


@router.post("/categorize")
def expenses_categorize(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    rebuild: str = Form(default=""),
):
    cfg, _ = load_expenses_config()
    rp = Path(cfg.categorization.rules_path)
    if not rp.exists():
        return RedirectResponse(url="/expenses/rules?error=Rules%20file%20not%20found", status_code=303)
    updated, _skipped_user = apply_rules_to_db(session=session, rules_path=rp, config=cfg.categorization, rebuild=(rebuild == "on"))
    return RedirectResponse(url=f"/expenses?ok=Categorized%20{updated}%20transactions", status_code=303)


@router.post("/normalize-monthly-installments")
def expenses_normalize_monthly_installments(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Targeted backfill for older imports: collapse any stored merchant_norm like
    'Monthly Installments (...)' into 'Apple' based on description patterns.
    """
    q = session.query(ExpenseTransaction).filter(
        func.lower(func.coalesce(ExpenseTransaction.merchant_norm, "")).like("monthly installment%")
        | func.lower(func.coalesce(ExpenseTransaction.description_norm, "")).like("%monthly installment%")
    )
    updated = 0
    for t in q:
        if (t.merchant_norm or "").strip() != "Apple":
            t.merchant_norm = "Apple"
            updated += 1
    session.commit()
    return RedirectResponse(url=f"/expenses?ok=Normalized%20{updated}%20Monthly%20Installment%20merchants%20to%20Apple", status_code=303)


@router.post("/normalize-bank-merchants")
def expenses_normalize_bank_merchants(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Targeted backfill for bank statement rollups: recompute merchant_norm using
    normalize_bank_merchant() for bank-like imports (especially Chase checking exports).
    """
    batch_ids = [
        b.id
        for b in session.query(ExpenseImportBatch).all()
        if (b.metadata_json or {}).get("format") == "chase_bank_csv"
    ]
    bank_account_ids = [a.id for a in session.query(ExpenseAccount.id).filter(ExpenseAccount.type == "BANK").all()]

    if not batch_ids and not bank_account_ids:
        return RedirectResponse(url="/expenses?ok=No%20bank%20imports%20found%20to%20normalize", status_code=303)

    q = session.query(ExpenseTransaction)
    if batch_ids and bank_account_ids:
        q = q.filter((ExpenseTransaction.import_batch_id.in_(batch_ids)) | (ExpenseTransaction.expense_account_id.in_(bank_account_ids)))
    elif batch_ids:
        q = q.filter(ExpenseTransaction.import_batch_id.in_(batch_ids))
    else:
        q = q.filter(ExpenseTransaction.expense_account_id.in_(bank_account_ids))

    updated = 0
    for t in q:
        new_merchant = normalize_bank_merchant(t.description_raw)
        if new_merchant and new_merchant != t.merchant_norm:
            t.merchant_norm = new_merchant
            updated += 1
    session.commit()
    return RedirectResponse(url=f"/expenses?ok=Normalized%20{updated}%20bank%20merchant%20names", status_code=303)


@router.post("/normalize-card-merchants")
def expenses_normalize_card_merchants(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Targeted backfill for credit card rollups: recompute merchant_norm using normalize_merchant()
    (helps collapse store codes like 'MCDONALD'S F6717 ...' into a single merchant).
    """
    bank_ids = [a.id for a in session.query(ExpenseAccount.id).filter(ExpenseAccount.type == "BANK").all()]
    q = session.query(ExpenseTransaction)
    if bank_ids:
        q = q.filter(~ExpenseTransaction.expense_account_id.in_(bank_ids))

    updated = 0
    for t in q:
        new_merchant = normalize_merchant(t.description_norm)
        if new_merchant and new_merchant != t.merchant_norm:
            t.merchant_norm = new_merchant
            updated += 1
    session.commit()
    return RedirectResponse(url=f"/expenses?ok=Normalized%20{updated}%20card%20merchant%20names", status_code=303)


@router.get("/transactions")
def expenses_transactions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    month: str = "",
    account_id: str = "",
    category: str = "",
    q: str = "",
    page: str = "1",
    page_size: str = "200",
):
    cfg, _ = load_expenses_config()
    accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()
    categories = list_categories(session, config=cfg.categorization)

    qry = session.query(ExpenseTransaction)
    qry = qry.options(joinedload(ExpenseTransaction.expense_account))
    account_sel, account_ids = _account_ids_from_param(session, account_id)
    year_i = _parse_int(year) or 0
    month_i = _parse_int(month) or 0
    page_i = _parse_int(page) or 1
    page_size_i = _parse_int(page_size) or 200

    if account_ids:
        qry = qry.filter(ExpenseTransaction.expense_account_id.in_(account_ids))
    if year_i and month_i:
        start, end = _month_bounds(year_i, month_i)
        qry = qry.filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
    elif year_i:
        start, end = dt.date(year_i, 1, 1), dt.date(year_i, 12, 31)
        qry = qry.filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
    if category.strip():
        cat = category.strip()
        qry = qry.filter(func.coalesce(ExpenseTransaction.category_user, ExpenseTransaction.category_system) == cat)
    if q.strip():
        like = f"%{q.strip()}%"
        qry = qry.filter(ExpenseTransaction.description_norm.ilike(like))

    total = int(qry.with_entities(func.count(ExpenseTransaction.id)).scalar() or 0)
    spend_total = (
        qry.with_entities(func.sum(-ExpenseTransaction.amount))
        .filter(ExpenseTransaction.amount < 0)
        .scalar()
        or 0
    )
    page_size_i = max(25, min(int(page_size_i), 500))
    page_i = max(1, int(page_i))
    rows = (
        qry.order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        .offset((page_i - 1) * page_size_i)
        .limit(page_size_i)
        .all()
    )

    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_transactions.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "expenses_active_tab": "transactions",
            "expenses_badge": "Transactions",
            "expenses_nav_year": str(year_i or dt.date.today().year),
            "today": dt.date.today().isoformat(),
            "accounts": accounts,
            "txns": rows,
            "total": total,
            "spend_total": float(spend_total or 0),
            "year": year_i,
            "month": month_i,
            "account_id": account_sel,
            "category": category,
            "q": q,
            "page": page_i,
            "page_size": page_size_i,
            "categories": categories,
        },
    )


@router.get("/recurring")
def expenses_recurring(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    min_months: str = "3",
    category: str = "",
    cadence: str = "",
):
    y = _parse_int(year) or dt.date.today().year
    mm = _parse_int(min_months) or 3
    all_items = detect_recurring(session=session, year=y, min_months=mm, include_income=False)
    category_sel = (category or "").strip()
    cadence_sel = (cadence or "").strip().upper()
    summary_by_cat: dict[str, dict[str, float | int]] = {}
    for it in all_items:
        c = (it.category or "Unknown").strip() or "Unknown"
        row = summary_by_cat.get(c) or {"monthly": 0.0, "merchant_count": 0}
        row["monthly"] = float(row["monthly"]) + float(it.monthly_equivalent)
        row["merchant_count"] = int(row["merchant_count"]) + 1
        summary_by_cat[c] = row
    cat_rows = [
        {"category": c, "monthly": money_2dp(Decimal(str(v["monthly"]))), "merchant_count": int(v["merchant_count"])}
        for c, v in summary_by_cat.items()
    ]
    cat_rows.sort(key=lambda r: (-float(r["monthly"]), str(r["category"])))

    # Summary grouped by recurring frequency (cadence).
    summary_by_cadence: dict[str, dict[str, float | int]] = {}
    for it in all_items:
        cad = (it.cadence or "UNKNOWN").strip().upper() or "UNKNOWN"
        row = summary_by_cadence.get(cad) or {"monthly": 0.0, "amount": 0.0, "merchant_count": 0}
        row["monthly"] = float(row["monthly"]) + float(it.monthly_equivalent)
        row["amount"] = float(row["amount"]) + float(it.amount)
        row["merchant_count"] = int(row["merchant_count"]) + 1
        summary_by_cadence[cad] = row
    cadence_rows = [
        {
            "cadence": cad,
            "monthly": money_2dp(Decimal(str(v["monthly"]))),
            "amount": money_2dp(Decimal(str(v["amount"]))),
            "merchant_count": int(v["merchant_count"]),
        }
        for cad, v in summary_by_cadence.items()
    ]
    cadence_rows.sort(key=lambda r: (-float(r["monthly"]), str(r["cadence"])))

    cadence_groups: list[dict[str, object]] = []
    for r in cadence_rows:
        cad = str(r["cadence"])
        group_items = [it for it in all_items if ((it.cadence or "UNKNOWN").strip().upper() or "UNKNOWN") == cad]
        group_items.sort(key=lambda it: (-float(it.monthly_equivalent), -float(it.amount), it.merchant))
        cadence_groups.append(
            {
                "cadence": cad,
                "est_monthly_total": r["monthly"],
                "period_total": r["amount"],
                "rows": group_items,
            }
        )
    recurring_monthly_total = money_2dp(
        sum((r["monthly"] for r in cadence_rows), start=Decimal("0.00"))  # type: ignore[arg-type]
    )
    recurring_amount_total = money_2dp(
        sum((r["amount"] for r in cadence_rows), start=Decimal("0.00"))  # type: ignore[arg-type]
    )
    recurring_items_total = sum((int(r["merchant_count"]) for r in cadence_rows), start=0)

    items = list(all_items)
    if category_sel:
        items = [it for it in items if ((it.category or "Unknown").strip() or "Unknown") == category_sel]
    if cadence_sel:
        items = [it for it in items if ((it.cadence or "UNKNOWN").strip().upper() or "UNKNOWN") == cadence_sel]
    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_recurring.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "expenses_active_tab": "recurring",
            "expenses_badge": "Recurring",
            "expenses_nav_year": str(y),
            "today": dt.date.today().isoformat(),
            "year": y,
            "min_months": mm,
            "items": items,
            "category": category_sel,
            "cadence": cadence_sel,
            "cat_rows": cat_rows,
            "cadence_rows": cadence_rows,
            "cadence_groups": cadence_groups,
            "recurring_monthly_total": recurring_monthly_total,
            "recurring_amount_total": recurring_amount_total,
            "recurring_items_total": recurring_items_total,
        },
    )


@router.get("/recurring/transactions")
def expenses_recurring_transactions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    min_months: str = "3",
    category: str = "",
    cadence: str = "",
):
    y = _parse_int(year) or dt.date.today().year
    mm = _parse_int(min_months) or 3
    category_sel = (category or "").strip()
    cadence_sel = (cadence or "").strip().upper()

    items = detect_recurring(session=session, year=y, min_months=mm, include_income=False)
    if category_sel:
        items = [it for it in items if ((it.category or "Unknown").strip() or "Unknown") == category_sel]
    if cadence_sel:
        items = [it for it in items if ((it.cadence or "UNKNOWN").strip().upper() or "UNKNOWN") == cadence_sel]

    merchants = sorted({(it.merchant or "").strip() for it in items if (it.merchant or "").strip()})
    merchant_lowers = sorted({m.lower() for m in merchants})

    start = dt.date(y, 1, 1)
    end = dt.date(y, 12, 31)

    txns: list[ExpenseTransaction] = []
    if merchant_lowers:
        q = (
            session.query(ExpenseTransaction)
            .options(joinedload(ExpenseTransaction.expense_account))
            .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
            .filter(ExpenseTransaction.amount < 0)
            .filter(func.lower(func.trim(func.coalesce(ExpenseTransaction.merchant_norm, ""))).in_(merchant_lowers))
            .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
            .limit(2000)
        )
        txns = q.all()

    spend_total = sum((-float(t.amount) for t in txns if float(t.amount) < 0), 0.0)

    from src.app.main import templates

    title_bits = ["Recurring transactions"]
    if cadence_sel:
        title_bits.append(cadence_sel)
    if category_sel:
        title_bits.append(category_sel)
    title = " — ".join(title_bits)

    return templates.TemplateResponse(
        "expenses_recurring_transactions.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "title": title,
            "year": y,
            "min_months": mm,
            "category": category_sel,
            "cadence": cadence_sel,
            "merchant_count": len(merchant_lowers),
            "txn_count": len(txns),
            "spend_total": float(spend_total or 0),
            "txns": txns,
        },
    )


def _recurring_charge_transactions(
    *,
    session: Session,
    year: int,
    min_months: int,
    category: str,
    cadence: str,
    limit: int = 20000,
) -> tuple[list[ExpenseTransaction], list[str]]:
    category_sel = (category or "").strip()
    cadence_sel = (cadence or "").strip().upper()

    items = detect_recurring(session=session, year=year, min_months=min_months, include_income=False)
    if category_sel:
        items = [it for it in items if ((it.category or "Unknown").strip() or "Unknown") == category_sel]
    if cadence_sel:
        items = [it for it in items if ((it.cadence or "UNKNOWN").strip().upper() or "UNKNOWN") == cadence_sel]

    merchants = sorted({(it.merchant or "").strip() for it in items if (it.merchant or "").strip()})
    merchant_lowers = sorted({m.lower() for m in merchants})
    if not merchant_lowers:
        return [], []

    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    q = (
        session.query(ExpenseTransaction)
        .options(joinedload(ExpenseTransaction.expense_account))
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .filter(ExpenseTransaction.amount < 0)
        .filter(func.lower(func.trim(func.coalesce(ExpenseTransaction.merchant_norm, ""))).in_(merchant_lowers))
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        .limit(int(limit))
    )
    return q.all(), merchant_lowers


@router.get("/recurring/transactions.csv")
def expenses_recurring_transactions_csv(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    min_months: str = "3",
    category: str = "",
    cadence: str = "",
):
    y = _parse_int(year) or dt.date.today().year
    mm = _parse_int(min_months) or 3
    txns, _merchant_lowers = _recurring_charge_transactions(
        session=session,
        year=y,
        min_months=mm,
        category=category,
        cadence=cadence,
        limit=50000,
    )

    def _acct_display(t: ExpenseTransaction) -> str:
        last4 = (t.account_last4_masked or "").strip() or ((t.expense_account.last4_masked or "").strip() if t.expense_account else "")
        if last4:
            return f"{t.institution} ****{last4}"
        return str(t.institution or "").strip() or "Unknown"

    def _effective_cat(t: ExpenseTransaction) -> str:
        c = (t.category_user or t.category_system or "").strip()
        return c if c else "Unknown"

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Posted", "Account", "Cardholder", "Merchant", "Description", "Category", "Amount"])
    for t in txns:
        amt = Decimal(str(t.amount)).quantize(Decimal("0.01"))
        w.writerow(
            [
                t.posted_date.isoformat() if t.posted_date else "",
                _acct_display(t),
                (t.cardholder_name or "").strip(),
                (t.merchant_norm or "").strip(),
                (t.description_norm or "").strip(),
                _effective_cat(t),
                f"{amt:f}",
            ]
        )

    cad = (cadence or "").strip().upper() or "ALL"
    fn = f"recurring_transactions_{y}_{cad}.csv"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


@router.get("/recurring/transactions.tsv")
def expenses_recurring_transactions_tsv(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    min_months: str = "3",
    category: str = "",
    cadence: str = "",
):
    y = _parse_int(year) or dt.date.today().year
    mm = _parse_int(min_months) or 3
    txns, _merchant_lowers = _recurring_charge_transactions(
        session=session,
        year=y,
        min_months=mm,
        category=category,
        cadence=cadence,
        limit=50000,
    )

    def _acct_display(t: ExpenseTransaction) -> str:
        last4 = (t.account_last4_masked or "").strip() or ((t.expense_account.last4_masked or "").strip() if t.expense_account else "")
        if last4:
            return f"{t.institution} ****{last4}"
        return str(t.institution or "").strip() or "Unknown"

    def _effective_cat(t: ExpenseTransaction) -> str:
        c = (t.category_user or t.category_system or "").strip()
        return c if c else "Unknown"

    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t", lineterminator="\n")
    w.writerow(["Posted", "Account", "Cardholder", "Merchant", "Description", "Category", "Amount"])
    for t in txns:
        amt = Decimal(str(t.amount)).quantize(Decimal("0.01"))
        w.writerow(
            [
                t.posted_date.isoformat() if t.posted_date else "",
                _acct_display(t),
                (t.cardholder_name or "").strip(),
                (t.merchant_norm or "").strip(),
                (t.description_norm or "").strip(),
                _effective_cat(t),
                f"{amt:f}",
            ]
        )

    cad = (cadence or "").strip().upper() or "ALL"
    fn = f"recurring_transactions_{y}_{cad}.tsv"
    return Response(
        content=buf.getvalue(),
        media_type="text/tab-separated-values; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


@router.get("/reports")
def expenses_reports(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    month: str = "",
    account_id: str = "",
):
    cfg, _ = load_expenses_config()
    y = _parse_int(year) or dt.date.today().year
    m_raw = _parse_int(month)
    m = m_raw if m_raw and 1 <= m_raw <= 12 else None
    account_sel, account_ids = _account_ids_from_param(session, account_id)
    cat = category_summary(session=session, year=y, month=m, account_ids=account_ids)
    merchants = merchants_by_spend(session=session, year=y, month=m, limit=200, account_ids=account_ids)
    cardholders = cardholders_by_spend(session=session, year=y, month=m, limit=50, account_ids=account_ids)
    opp = opportunities(session=session, year=y, month=m, account_ids=account_ids)
    budgets = budget_vs_actual(cat.rows, cfg.categorization.budgets_monthly) if m else []
    total_spend = sum((r.spend for r in cat.rows), start=Decimal("0"))
    total_income = sum((r.income for r in cat.rows), start=Decimal("0"))
    total_net = sum((r.net for r in cat.rows), start=Decimal("0"))
    total_txn_count = sum((int(getattr(r, "txn_count", 0) or 0) for r in cat.rows), start=0)

    from src.app.main import templates
    accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()

    return templates.TemplateResponse(
        "expenses_reports.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "expenses_active_tab": "reports",
            "expenses_badge": "Reports",
            "expenses_nav_year": str(y),
            "today": dt.date.today().isoformat(),
            "error": request.query_params.get("error"),
            "ok": request.query_params.get("ok"),
            "year": y,
            "month": (m or 0),
            "account_id": account_sel,
            "accounts": accounts,
            "cat_rows": cat.rows,
            "totals_spend": total_spend,
            "totals_income": total_income,
            "totals_net": total_net,
            "totals_txn_count": total_txn_count,
            "merchant_rows": merchants,
            "cardholder_rows": cardholders,
            "opportunities": opp,
            "budget_rows": budgets,
        },
    )


@router.get("/category")
def expenses_category_detail(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = "",
    year: str = "",
    month: str = "",
    account_id: str = "",
):
    category = (name or "").strip()
    if not category:
        return RedirectResponse(url="/expenses/reports?error=Missing%20category", status_code=303)

    cfg, _ = load_expenses_config()
    y = _parse_int(year) or dt.date.today().year
    m_raw = _parse_int(month)
    m = m_raw if m_raw and 1 <= m_raw <= 12 else None
    account_sel, account_ids = _account_ids_from_param(session, account_id)
    categories = list_categories(session, config=cfg.categorization)

    start = dt.date(y, 1, 1)
    end = dt.date(y, 12, 31)
    if m:
        start, end = _month_bounds(y, m)

    qry = (
        session.query(ExpenseTransaction)
        .options(joinedload(ExpenseTransaction.expense_account))
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .filter(func.coalesce(ExpenseTransaction.category_user, ExpenseTransaction.category_system) == category)
    )
    if account_ids:
        qry = qry.filter(ExpenseTransaction.expense_account_id.in_(account_ids))

    rows = qry.order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc()).limit(500).all()
    spend_total = (
        qry.with_entities(func.sum(-ExpenseTransaction.amount))
        .filter(ExpenseTransaction.amount < 0)
        .scalar()
        or 0
    )
    income_total = (
        qry.with_entities(func.sum(ExpenseTransaction.amount))
        .filter(ExpenseTransaction.amount > 0)
        .scalar()
        or 0
    )
    # Merchant rollup for this category in the selected period (charges only),
    # with inline access to recurring settings.
    from src.db.models import ExpenseMerchantSetting

    mq = (
        session.query(
            ExpenseTransaction.merchant_norm,
            func.sum(-ExpenseTransaction.amount).label("spend"),
            func.count(ExpenseTransaction.id).label("txn_count"),
        )
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .filter(func.coalesce(ExpenseTransaction.category_user, ExpenseTransaction.category_system) == category)
        .filter(ExpenseTransaction.amount < 0)
    )
    if account_ids:
        mq = mq.filter(ExpenseTransaction.expense_account_id.in_(account_ids))
    merchants_in_category = (
        mq.group_by(ExpenseTransaction.merchant_norm)
        .order_by(func.sum(-ExpenseTransaction.amount).desc(), ExpenseTransaction.merchant_norm.asc())
        .limit(50)
        .all()
    )
    keys = [merchant_key(m or "") for (m, _s, _c) in merchants_in_category if (m or "").strip()]
    settings_by_key: dict[str, ExpenseMerchantSetting] = {}
    if keys:
        for s in (
            session.query(ExpenseMerchantSetting)
            .filter(ExpenseMerchantSetting.merchant_key.in_(list(dict.fromkeys(keys))))
            .all()
        ):
            settings_by_key[str(s.merchant_key)] = s
    merchant_summary_rows: list[dict[str, object]] = []
    for mname, spend, txn_count in merchants_in_category:
        mdisp = (mname or "").strip() or "Unknown"
        k = merchant_key(mdisp)
        s = settings_by_key.get(k)
        merchant_summary_rows.append(
            {
                "merchant": mdisp,
                "spend": float(spend or 0),
                "txn_count": int(txn_count or 0),
                "recurring_enabled": bool(getattr(s, "recurring_enabled", False)),
                "cadence": str(getattr(s, "cadence", "UNKNOWN") or "UNKNOWN"),
            }
        )

    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_category.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "category": category,
            "year": y,
            "month": (m or 0),
            "account_id": account_sel,
            "txns": rows,
            "categories": categories,
            "spend_total": float(spend_total or 0),
            "income_total": float(income_total or 0),
            "merchant_summary_rows": merchant_summary_rows,
        },
    )


@router.post("/txn/category")
def expenses_set_txn_category(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    txn_id: str = Form(...),
    category: str = Form(default=""),
    new_category: str = Form(default=""),
    return_to: str = Form(default="/expenses"),
):
    cfg, _ = load_expenses_config()
    t = session.query(ExpenseTransaction).filter(ExpenseTransaction.txn_id == txn_id).one_or_none()
    if t is None:
        return RedirectResponse(url=f"{return_to}?error=Transaction%20not%20found", status_code=303)

    prev_effective = (t.category_user or t.category_system or "Unknown").strip() or "Unknown"
    chosen = (new_category or "").strip() or (category or "").strip()
    if chosen:
        chosen = ensure_category(session, name=chosen)
        t.category_user = chosen
    else:
        # Clear user override.
        t.category_user = None

    if chosen:
        learn_unknown_merchant_category(
            session=session,
            merchant_norm=t.merchant_norm or "",
            category=chosen,
            from_category=prev_effective,
        )
    session.commit()
    return RedirectResponse(url=_safe_return_to(return_to, default="/expenses/transactions"), status_code=303)


@router.get("/merchants")
def expenses_merchants(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    month: str = "",
    limit: str = "200",
    account_id: str = "",
):
    y = _parse_int(year) or dt.date.today().year
    m_raw = _parse_int(month)
    m = m_raw if m_raw and 1 <= m_raw <= 12 else None
    lim = _parse_int(limit) or 200
    account_sel, account_ids = _account_ids_from_param(session, account_id)
    rows = merchants_by_spend(session=session, year=y, month=m, limit=lim, account_ids=account_ids)
    accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_merchants.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "year": y,
            "month": (m or 0),
            "limit": lim,
            "account_id": account_sel,
            "accounts": accounts,
            "rows": rows,
        },
    )


@router.get("/merchant")
def expenses_merchant_detail(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = "",
    year: str = "",
    month: str = "",
    account_id: str = "",
    return_to: str = "",
):
    merchant = (name or "").strip()
    if not merchant:
        return RedirectResponse(url="/expenses/merchants?error=Missing%20merchant", status_code=303)
    y = _parse_int(year) or dt.date.today().year
    m_raw = _parse_int(month)
    m = m_raw if m_raw and 1 <= m_raw <= 12 else None
    account_sel, account_ids = _account_ids_from_param(session, account_id)

    start = dt.date(y, 1, 1)
    end = dt.date(y, 12, 31)
    if m:
        start, end = _month_bounds(y, m)

    qry = (
        session.query(ExpenseTransaction)
        .options(joinedload(ExpenseTransaction.expense_account))
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
    )
    if merchant == "Apple":
        # Backward compatibility: include older imports whose merchant_norm was derived from installment text.
        qry = qry.filter(
            (ExpenseTransaction.merchant_norm == "Apple")
            | func.lower(func.coalesce(ExpenseTransaction.description_norm, "")).like("%monthly installment%")
            | func.lower(func.coalesce(ExpenseTransaction.merchant_norm, "")).like("monthly installment%")
        )
    else:
        qry = qry.filter(func.lower(func.trim(func.coalesce(ExpenseTransaction.merchant_norm, ""))) == merchant.lower())
    if account_ids:
        qry = qry.filter(ExpenseTransaction.expense_account_id.in_(account_ids))
    rows = qry.limit(800).all()
    spend_total = (
        qry.with_entities(func.sum(-ExpenseTransaction.amount))
        .filter(ExpenseTransaction.amount < 0)
        .scalar()
        or 0
    )
    income_total = (
        qry.with_entities(func.sum(ExpenseTransaction.amount))
        .filter(ExpenseTransaction.amount > 0)
        .scalar()
        or 0
    )
    cfg, _ = load_expenses_config()
    categories = list_categories(session, config=cfg.categorization)
    # Show the dominant category for charge transactions in the selected period.
    excluded = {"Transfers", "Income", "Payments", "Merchant Credits"}
    by_cat: dict[str, float] = {}
    for t in rows:
        eff = (t.category_user or t.category_system or "Unknown").strip() or "Unknown"
        if eff in excluded:
            continue
        if float(t.amount) >= 0:
            continue
        by_cat[eff] = by_cat.get(eff, 0.0) + (-float(t.amount))
    merchant_category = "Unknown"
    if by_cat:
        merchant_category = sorted(by_cat.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    recurring_setting = get_merchant_setting(session, merchant=merchant)

    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_merchant.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "merchant": merchant,
            "year": y,
            "month": (m or 0),
            "account_id": account_sel,
            "return_to": return_to,
            "spend_total": float(spend_total or 0),
            "income_total": float(income_total or 0),
            "txns": rows,
            "categories": categories,
            "merchant_category": merchant_category,
            "recurring_setting": recurring_setting,
        },
    )


@router.post("/merchant/category")
def expenses_set_merchant_category(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    merchant: str = Form(...),
    category: str = Form(default=""),
    new_category: str = Form(default=""),
    return_to: str = Form(default="/expenses/merchants"),
):
    cfg, _ = load_expenses_config()
    m = (merchant or "").strip()
    if not m:
        return RedirectResponse(url=f"{return_to}?error=Missing%20merchant", status_code=303)
    chosen = (new_category or "").strip() or (category or "").strip()
    if not chosen:
        return RedirectResponse(url=f"{return_to}?error=Missing%20category", status_code=303)
    chosen = ensure_category(session, name=chosen)
    set_merchant_category(session=session, merchant=m, category=chosen)
    session.commit()
    return RedirectResponse(url=_safe_return_to(return_to, default="/expenses/merchants"), status_code=303)


@router.post("/merchant/recurring")
def expenses_set_merchant_recurring(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    merchant: str = Form(...),
    recurring_enabled: str = Form(default=""),
    cadence: str = Form(default="UNKNOWN"),
    return_to: str = Form(default="/expenses/merchants"),
):
    m = (merchant or "").strip()
    if not m:
        return RedirectResponse(url=f"{return_to}?error=Missing%20merchant", status_code=303)
    enabled = bool((recurring_enabled or "").strip())
    try:
        upsert_merchant_setting(session, merchant=m, recurring_enabled=enabled, cadence=cadence)
        session.commit()
    except ValueError as e:
        safe = _safe_return_to(return_to, default="/expenses/merchants")
        return RedirectResponse(url=f"{safe}?error={str(e)}", status_code=303)
    return RedirectResponse(url=_safe_return_to(return_to, default="/expenses/merchants"), status_code=303)


@router.get("/cardholders")
def expenses_cardholders(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    year: str = "",
    month: str = "",
    limit: str = "100",
    account_id: str = "",
):
    y = _parse_int(year) or dt.date.today().year
    m_raw = _parse_int(month)
    m = m_raw if m_raw and 1 <= m_raw <= 12 else None
    lim = _parse_int(limit) or 100
    account_sel, account_ids = _account_ids_from_param(session, account_id)
    rows = cardholders_by_spend(session=session, year=y, month=m, limit=lim, account_ids=account_ids)
    accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_cardholders.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "year": y,
            "month": (m or 0),
            "limit": lim,
            "account_id": account_sel,
            "accounts": accounts,
            "rows": rows,
        },
    )


@router.get("/cardholder")
def expenses_cardholder_detail(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = "",
    year: str = "",
    month: str = "",
    account_id: str = "",
):
    cardholder = (name or "").strip()
    if not cardholder:
        return RedirectResponse(url="/expenses/cardholders?error=Missing%20cardholder", status_code=303)
    y = _parse_int(year) or dt.date.today().year
    m_raw = _parse_int(month)
    m = m_raw if m_raw and 1 <= m_raw <= 12 else None
    account_sel, account_ids = _account_ids_from_param(session, account_id)

    start = dt.date(y, 1, 1)
    end = dt.date(y, 12, 31)
    if m:
        start, end = _month_bounds(y, m)

    qry = (
        session.query(ExpenseTransaction)
        .options(joinedload(ExpenseTransaction.expense_account))
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .filter(
            func.lower(func.trim(func.coalesce(ExpenseTransaction.cardholder_name, "")))
            == ("" if cardholder == "Unknown" else cardholder.strip().lower())
        )
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
    )
    if account_ids:
        qry = qry.filter(ExpenseTransaction.expense_account_id.in_(account_ids))

    excluded = {"Transfers", "Income", "Payments", "Merchant Credits"}
    rows = [t for t in qry.limit(1200).all() if (t.category_user or t.category_system or "Unknown") not in excluded and float(t.amount) < 0]
    spend_total = sum((-float(t.amount) for t in rows if float(t.amount) < 0), 0.0)

    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_cardholder.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "cardholder": cardholder,
            "year": y,
            "month": (m or 0),
            "account_id": account_sel,
            "spend_total": float(spend_total or 0),
            "txns": rows,
        },
    )


@router.get("/rules")
def expenses_rules(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    cfg, _ = load_expenses_config()
    rp = Path(cfg.categorization.rules_path)
    content = rp.read_text() if rp.exists() else ""
    parse_error = None
    if rp.exists():
        try:
            load_rules(rp, defaults=cfg.categorization)
        except Exception as e:
            parse_error = f"{type(e).__name__}: {e}"
    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_rules.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "expenses_active_tab": "rules",
            "expenses_badge": "Rules",
            "expenses_nav_year": dt.date.today().strftime("%Y"),
            "today": dt.date.today().isoformat(),
            "rules_path": str(rp),
            "rules_exists": rp.exists(),
            "rules_content": content,
            "parse_error": parse_error,
            "error": request.query_params.get("error"),
            "ok": request.query_params.get("ok"),
        },
    )


@router.get("/purge")
def expenses_purge(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "expenses_purge.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
        },
    )


@router.post("/accounts/scope")
def expenses_account_scope_update(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: str = Form(...),
    scope: str = Form(...),
):
    aid = _parse_int(account_id) or 0
    if not aid:
        return RedirectResponse(url="/expenses?error=Missing%20account", status_code=303)
    scope_norm = (scope or "").strip().upper()
    allowed = {"PERSONAL", "FAMILY", "BUSINESS"}
    if scope_norm not in allowed:
        return RedirectResponse(url="/expenses?error=Invalid%20scope", status_code=303)
    acct = session.query(ExpenseAccount).filter(ExpenseAccount.id == aid).one_or_none()
    if acct is None:
        return RedirectResponse(url="/expenses?error=Account%20not%20found", status_code=303)
    acct.scope = scope_norm
    session.commit()
    return RedirectResponse(url="/expenses?ok=Updated%20account%20scope", status_code=303)


@router.post("/purge/account")
def expenses_purge_account(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: str = Form(...),
    confirm: str = Form(default=""),
):
    if (confirm or "").strip().upper() != "PURGE":
        return RedirectResponse(url="/expenses/purge?error=Type%20PURGE%20to%20confirm", status_code=303)
    aid = _parse_int(account_id) or 0
    if not aid:
        return RedirectResponse(url="/expenses/purge?error=Missing%20account", status_code=303)
    res = purge_account_data(session=session, account_id=aid)
    session.commit()
    return RedirectResponse(
        url=f"/expenses?ok=Purged%20account%20{aid}%20(%2D{res['transactions_deleted']}%20txns)",
        status_code=303,
    )


@router.post("/purge/all")
def expenses_purge_all(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    confirm: str = Form(default=""),
    include_rules: str = Form(default=""),
    include_categories: str = Form(default=""),
):
    if (confirm or "").strip().upper() != "PURGE":
        return RedirectResponse(url="/expenses/purge?error=Type%20PURGE%20to%20confirm", status_code=303)
    res = purge_all_expenses_data(
        session=session,
        include_rules=(include_rules == "on"),
        include_categories=(include_categories == "on"),
    )
    session.commit()
    return RedirectResponse(
        url=f"/expenses?ok=Purged%20all%20expense%20data%20(%2D{res['transactions_deleted']}%20txns)",
        status_code=303,
    )


@router.post("/rules/init")
def expenses_rules_init(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    cfg, _ = load_expenses_config()
    rp = Path(cfg.categorization.rules_path)
    try:
        write_starter_rules(rp, config=cfg.categorization, force=False)
    except Exception as e:
        return RedirectResponse(url=f"/expenses/rules?error={type(e).__name__}: {e}", status_code=303)
    return RedirectResponse(url="/expenses/rules?ok=Created%20starter%20rules", status_code=303)


@router.post("/rules/save")
def expenses_rules_save(
    rules_text: str = Form(...),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    cfg, _ = load_expenses_config()
    rp = Path(cfg.categorization.rules_path)
    try:
        # Validate parse first.
        tmp = rp.with_suffix(".tmp.yaml")
        tmp.write_text(rules_text)
        load_rules(tmp, defaults=cfg.categorization)
        rp.write_text(rules_text)
        tmp.unlink(missing_ok=True)  # type: ignore[call-arg]
    except Exception as e:
        try:
            tmp.unlink(missing_ok=True)  # type: ignore[call-arg]
        except Exception:
            pass
        return RedirectResponse(url=f"/expenses/rules?error={type(e).__name__}: {e}", status_code=303)
    return RedirectResponse(url="/expenses/rules?ok=Saved%20rules", status_code=303)
