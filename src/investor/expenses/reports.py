from __future__ import annotations

import csv
import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional

from sqlalchemy.orm import Session

from src.db.models import ExpenseTransaction
from src.investor.expenses.models import BudgetRow, CardholderSpendRow, MerchantSpendRow, ReportRow
from src.investor.expenses.normalize import money_2dp


def _year_month(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _effective_category(t: ExpenseTransaction) -> str:
    c = (t.category_user or t.category_system or "").strip()
    return c if c else "Unknown"


_PAYMENT_LIKE_CATEGORIES = {"payments", "merchant credits", "income"}


def _is_payment_like_category(category: str) -> bool:
    """
    Returns True for categories that should not be counted as "spend"/charges in rollups,
    e.g. bill payments and credits.
    """
    s = (category or "").strip().casefold()
    if not s:
        return False
    if s in _PAYMENT_LIKE_CATEGORIES:
        return True
    # Common user-defined category used for bank->card bill payments.
    if s in {"credit card payment", "card payment", "cc payment"}:
        return True
    if ("payment" in s) and any(k in s for k in ["credit card", "card", "cc"]):
        return True
    return False


def _bucket_spend_payment(*, category: str, amount: Decimal) -> tuple[Decimal, Decimal]:
    """
    Returns (spend, payment) in canonical reporting terms.

    - spend: money out (charges), positive number
    - payment: money in (payments/credits/income/refunds), positive number

    This is category-aware to be robust to legacy imports with inconsistent signs.
    """
    cat = (category or "Unknown").strip()
    amt = money_2dp(amount)
    if _is_payment_like_category(cat):
        return Decimal("0"), money_2dp(abs(amt))
    if amt < 0:
        return money_2dp(-amt), Decimal("0")
    if amt > 0:
        # Refunds/credits that were categorized into a spend category: treat as payment in rollups.
        return Decimal("0"), money_2dp(amt)
    return Decimal("0"), Decimal("0")


@dataclass(frozen=True)
class ExpenseReport:
    scope: str
    rows: list[ReportRow]


def monthly_summary(
    *,
    session: Session,
    year: int,
    month: Optional[int] = None,
    account_id: Optional[int] = None,
    account_ids: Optional[Iterable[int]] = None,
) -> ExpenseReport:
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    if month:
        start = dt.date(year, month, 1)
        end = dt.date(year, month, 28) + dt.timedelta(days=4)
        end = end.replace(day=1) - dt.timedelta(days=1)

    q = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    )
    if account_ids:
        q = q.filter(ExpenseTransaction.expense_account_id.in_(list(account_ids)))
    elif account_id:
        q = q.filter(ExpenseTransaction.expense_account_id == int(account_id))
    by_key: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"spend": Decimal("0"), "income": Decimal("0")})
    payment_groups: dict[tuple, dict[str, Decimal]] = defaultdict(lambda: {"pos": Decimal("0"), "neg": Decimal("0")})
    for t in q:
        key = _year_month(t.posted_date) if month is None else _effective_category(t)
        cat = _effective_category(t)
        amt = Decimal(str(t.amount))
        if _is_payment_like_category(cat):
            abs_amt = money_2dp(abs(amt))
            fp = (
                key,
                int(t.expense_account_id),
                t.posted_date.isoformat() if t.posted_date else "",
                (t.merchant_norm or "").strip().upper(),
                (t.description_norm or "").strip().upper(),
                str(abs_amt),
                cat,
            )
            if amt > 0:
                payment_groups[fp]["pos"] = money_2dp(payment_groups[fp]["pos"] + money_2dp(amt))
            elif amt < 0:
                payment_groups[fp]["neg"] = money_2dp(payment_groups[fp]["neg"] + abs_amt)
            continue

        spend, payment = _bucket_spend_payment(category=cat, amount=amt)
        by_key[key]["spend"] += spend
        by_key[key]["income"] += payment

    for fp, sums in payment_groups.items():
        key = fp[0]
        pos = money_2dp(sums.get("pos", Decimal("0")))
        neg = money_2dp(sums.get("neg", Decimal("0")))
        by_key[key]["income"] += pos if pos > 0 else neg

    rows: list[ReportRow] = []
    for k, v in by_key.items():
        spend = money_2dp(v["spend"])
        income = money_2dp(v["income"])
        net = money_2dp(spend - income)
        rows.append(ReportRow(key=str(k), spend=spend, income=income, net=net))

    rows.sort(key=lambda r: r.key)
    scope = f"{year}" if month is None else f"{year}-{month:02d}"
    return ExpenseReport(scope=scope, rows=rows)


def category_summary(
    *,
    session: Session,
    year: int,
    month: Optional[int] = None,
    account_id: Optional[int] = None,
    account_ids: Optional[Iterable[int]] = None,
) -> ExpenseReport:
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    if month:
        start = dt.date(year, month, 1)
        end = dt.date(year, month, 28) + dt.timedelta(days=4)
        end = end.replace(day=1) - dt.timedelta(days=1)

    q = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    )
    if account_ids:
        q = q.filter(ExpenseTransaction.expense_account_id.in_(list(account_ids)))
    elif account_id:
        q = q.filter(ExpenseTransaction.expense_account_id == int(account_id))
    by_cat: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"spend": Decimal("0"), "income": Decimal("0")})
    by_cat_cnt: dict[str, int] = defaultdict(int)
    payment_groups: dict[tuple, dict[str, Decimal]] = defaultdict(lambda: {"pos": Decimal("0"), "neg": Decimal("0")})
    for t in q:
        cat = _effective_category(t)
        amt = Decimal(str(t.amount))
        if _is_payment_like_category(cat):
            abs_amt = money_2dp(abs(amt))
            fp = (
                int(t.expense_account_id),
                t.posted_date.isoformat() if t.posted_date else "",
                (t.merchant_norm or "").strip().upper(),
                (t.description_norm or "").strip().upper(),
                str(abs_amt),
                cat,
            )
            if amt > 0:
                payment_groups[fp]["pos"] = money_2dp(payment_groups[fp]["pos"] + money_2dp(amt))
            elif amt < 0:
                payment_groups[fp]["neg"] = money_2dp(payment_groups[fp]["neg"] + abs_amt)
            continue

        spend, payment = _bucket_spend_payment(category=cat, amount=amt)
        by_cat[cat]["spend"] += spend
        by_cat[cat]["income"] += payment
        if spend > 0:
            by_cat_cnt[cat] += 1

    for fp, sums in payment_groups.items():
        cat = fp[-1]
        pos = money_2dp(sums.get("pos", Decimal("0")))
        neg = money_2dp(sums.get("neg", Decimal("0")))
        by_cat[cat]["income"] += pos if pos > 0 else neg

    rows: list[ReportRow] = []
    for c, v in by_cat.items():
        spend = money_2dp(v["spend"])
        income = money_2dp(v["income"])
        net = money_2dp(spend - income)
        rows.append(ReportRow(key=c, spend=spend, income=income, net=net, txn_count=int(by_cat_cnt.get(c, 0) or 0)))

    rows.sort(key=lambda r: (-float(r.spend), r.key))
    scope = f"{year}" if month is None else f"{year}-{month:02d}"
    return ExpenseReport(scope=scope, rows=rows)


def top_merchants(
    *,
    session: Session,
    year: int,
    month: Optional[int] = None,
    limit: int = 25,
    exclude_categories: Optional[set[str]] = None,
) -> list[ReportRow]:
    # Backward-compatible wrapper.
    rows = merchants_by_spend(
        session=session,
        year=year,
        month=month,
        limit=limit,
        exclude_categories=exclude_categories,
    )
    return [ReportRow(key=r.merchant, spend=r.spend, income=Decimal("0"), net=money_2dp(-r.spend)) for r in rows]


def merchants_by_spend(
    *,
    session: Session,
    year: int,
    month: Optional[int] = None,
    limit: int = 50,
    exclude_categories: Optional[set[str]] = None,
    account_id: Optional[int] = None,
    account_ids: Optional[Iterable[int]] = None,
) -> list[MerchantSpendRow]:
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    if month:
        start = dt.date(year, month, 1)
        end = dt.date(year, month, 28) + dt.timedelta(days=4)
        end = end.replace(day=1) - dt.timedelta(days=1)

    q = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    )
    if account_ids:
        q = q.filter(ExpenseTransaction.expense_account_id.in_(list(account_ids)))
    elif account_id:
        q = q.filter(ExpenseTransaction.expense_account_id == int(account_id))
    excluded = exclude_categories or {"Transfers", "Income", "Payments", "Merchant Credits"}

    def _key(name: str) -> str:
        s = " ".join((name or "").strip().split())
        return s.casefold()

    def _rank(display: str) -> int:
        s = (display or "").strip()
        if not s or s == "Unknown":
            return 0
        has_upper = any(ch.isalpha() and ch.isupper() for ch in s)
        has_lower = any(ch.isalpha() and ch.islower() for ch in s)
        if has_upper and has_lower:
            return 3  # mixed-case preferred
        if has_upper and not has_lower:
            return 2  # ALL CAPS
        return 1  # other

    by_m: dict[str, tuple[Decimal, int, str]] = defaultdict(lambda: (Decimal("0"), 0, "Unknown"))
    by_mc: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(lambda: Decimal("0")))
    for t in q:
        cat = _effective_category(t)
        if cat in excluded:
            continue
        amt = Decimal(str(t.amount))
        if amt >= 0:
            continue
        merchant = (t.merchant_norm or "").strip() or "Unknown"
        # Collapse common Amazon-family labels (older imports may not have normalized these).
        dn = (t.description_norm or "").lower()
        mn = (merchant or "").lower()
        if any(k in dn for k in ["prime video", "primevideo", "prime video channels", "kindle", "amzn digital", "amazon digital", "audible", "amazon music"]):
            merchant = "Amazon"
        elif any(k in mn for k in ["prime video", "kindle", "amzn digital", "audible", "amazon music"]):
            merchant = "Amazon"
        if merchant.lower().startswith("monthly installment") or "monthly installment" in (t.description_norm or "").lower():
            merchant = "Apple"
        spend = money_2dp(-amt)
        k = _key(merchant) or "unknown"
        total, cnt, best = by_m[k]
        total = money_2dp(total + spend)
        cnt = cnt + 1
        if _rank(merchant) > _rank(best):
            best = merchant
        by_m[k] = (total, cnt, best)
        by_mc[k][cat] = money_2dp(by_mc[k][cat] + spend)

    rows: list[MerchantSpendRow] = []
    for k, (v, cnt, best_m) in by_m.items():
        cats = by_mc.get(k) or {}
        best = "Unknown"
        best_spend = Decimal("-1")
        for c, s in cats.items():
            if s > best_spend or (s == best_spend and c < best):
                best = c
                best_spend = s
        rows.append(MerchantSpendRow(merchant=best_m, spend=money_2dp(v), txn_count=cnt, category=best))
    rows.sort(key=lambda r: (-float(r.spend), r.merchant))
    return rows[: max(0, int(limit))]


def cardholders_by_spend(
    *,
    session: Session,
    year: int,
    month: Optional[int] = None,
    limit: int = 50,
    exclude_categories: Optional[set[str]] = None,
    account_id: Optional[int] = None,
    account_ids: Optional[Iterable[int]] = None,
) -> list[CardholderSpendRow]:
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    if month:
        start = dt.date(year, month, 1)
        end = dt.date(year, month, 28) + dt.timedelta(days=4)
        end = end.replace(day=1) - dt.timedelta(days=1)

    q = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    )
    if account_ids:
        q = q.filter(ExpenseTransaction.expense_account_id.in_(list(account_ids)))
    elif account_id:
        q = q.filter(ExpenseTransaction.expense_account_id == int(account_id))
    excluded = exclude_categories or {"Transfers", "Income", "Payments", "Merchant Credits"}

    def _key(name: str) -> str:
        s = " ".join((name or "").strip().split())
        return s.casefold()

    def _rank(display: str) -> int:
        s = (display or "").strip()
        if not s or s == "Unknown":
            return 0
        has_upper = any(ch.isalpha() and ch.isupper() for ch in s)
        has_lower = any(ch.isalpha() and ch.islower() for ch in s)
        if has_upper and has_lower:
            return 3  # mixed-case preferred
        if has_upper and not has_lower:
            return 2  # ALL CAPS
        return 1  # other

    by_key: dict[str, tuple[Decimal, int, str]] = defaultdict(lambda: (Decimal("0"), 0, "Unknown"))
    for t in q:
        if _effective_category(t) in excluded:
            continue
        amt = Decimal(str(t.amount))
        if amt >= 0:
            continue
        raw = (t.cardholder_name or "").strip()
        disp = raw or "Unknown"
        k = _key(raw) or "unknown"
        total, cnt, best = by_key[k]
        total = money_2dp(total + money_2dp(-amt))
        cnt = cnt + 1
        if _rank(disp) > _rank(best):
            best = disp
        by_key[k] = (total, cnt, best)

    rows = [CardholderSpendRow(cardholder=best, spend=money_2dp(v), txn_count=cnt) for _k, (v, cnt, best) in by_key.items()]
    rows.sort(key=lambda r: (-float(r.spend), r.cardholder))
    return rows[: max(0, int(limit))]


def write_csv(rows: Iterable[ReportRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["key", "spend", "income", "net"])
        w.writeheader()
        for r in rows:
            w.writerow({"key": r.key, "spend": str(r.spend), "income": str(r.income), "net": str(r.net)})


def render_simple_html(title: str, rows: Iterable[ReportRow]) -> str:
    tr = "\n".join(
        f"<tr><td>{r.key}</td><td style='text-align:right'>{r.spend}</td><td style='text-align:right'>{r.income}</td><td style='text-align:right'>{r.net}</td></tr>"
        for r in rows
    )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, system-ui, sans-serif; padding: 16px; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 900px; }}
    th, td {{ border-bottom: 1px solid #ddd; padding: 6px 8px; }}
    th {{ text-align: left; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <table>
    <thead><tr><th>Key</th><th>Spend</th><th>Income</th><th>Net</th></tr></thead>
    <tbody>
      {tr}
    </tbody>
  </table>
</body>
</html>
"""


def format_table(rows: list[ReportRow], *, headers: tuple[str, str, str, str] = ("Key", "Spend", "Income", "Net")) -> str:
    if not rows:
        return "(no rows)"
    k_w = max(len(headers[0]), *(len(r.key) for r in rows))
    s_w = max(len(headers[1]), *(len(str(r.spend)) for r in rows))
    i_w = max(len(headers[2]), *(len(str(r.income)) for r in rows))
    n_w = max(len(headers[3]), *(len(str(r.net)) for r in rows))

    def line(k: str, s: str, i: str, n: str) -> str:
        return f"{k:<{k_w}}  {s:>{s_w}}  {i:>{i_w}}  {n:>{n_w}}"

    out = [line(*headers), line("-" * k_w, "-" * s_w, "-" * i_w, "-" * n_w)]
    for r in rows:
        out.append(line(r.key, str(r.spend), str(r.income), str(r.net)))
    return "\n".join(out)


def budget_vs_actual(category_rows: list[ReportRow], budgets_monthly: dict[str, float]) -> list[BudgetRow]:
    if not budgets_monthly:
        return []
    out: list[BudgetRow] = []
    b = {k.strip(): Decimal(str(v)) for k, v in budgets_monthly.items() if k and v is not None}
    for r in category_rows:
        cat = r.key
        if cat not in b:
            continue
        budget = money_2dp(b[cat])
        spend = money_2dp(r.spend)
        out.append(BudgetRow(category=cat, spend=spend, budget=budget, over_under=money_2dp(budget - spend)))
    out.sort(key=lambda x: x.category)
    return out


def opportunities(
    *,
    session: Session,
    year: int,
    month: Optional[int] = None,
    top_n: int = 5,
    account_id: Optional[int] = None,
    account_ids: Optional[Iterable[int]] = None,
) -> list[str]:
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    if month:
        start = dt.date(year, month, 1)
        end = dt.date(year, month, 28) + dt.timedelta(days=4)
        end = end.replace(day=1) - dt.timedelta(days=1)

    q = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    )
    if account_ids:
        q = q.filter(ExpenseTransaction.expense_account_id.in_(list(account_ids)))
    elif account_id:
        q = q.filter(ExpenseTransaction.expense_account_id == int(account_id))

    excluded = {"Transfers", "Income", "Payments", "Merchant Credits"}
    discretionary = {"Dining", "Shopping", "Travel", "Subscriptions"}

    by_cat: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    by_disc_merchant: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    small_freq: dict[str, tuple[int, Decimal]] = defaultdict(lambda: (0, Decimal("0")))

    for t in q:
        amt = Decimal(str(t.amount))
        if amt >= 0:
            continue
        spend = money_2dp(-amt)
        cat = _effective_category(t)
        merch = t.merchant_norm or "Unknown"
        if cat not in excluded:
            by_cat[cat] += spend
        if cat in discretionary:
            by_disc_merchant[merch] += spend
        if Decimal("1.00") <= spend <= Decimal("10.00"):
            cnt, total = small_freq[merch]
            small_freq[merch] = (cnt + 1, money_2dp(total + spend))

    lines: list[str] = []
    top_cats = sorted(by_cat.items(), key=lambda kv: (-float(kv[1]), kv[0]))[:top_n]
    if top_cats:
        lines.append("Biggest spend categories: " + ", ".join(f"{c} (${v})" for c, v in top_cats))

    top_merch = sorted(by_disc_merchant.items(), key=lambda kv: (-float(kv[1]), kv[0]))[:top_n]
    if top_merch:
        lines.append("Top discretionary merchants: " + ", ".join(f"{m} (${v})" for m, v in top_merch))

    small = [(m, cnt, total) for m, (cnt, total) in small_freq.items() if cnt >= 8]
    small.sort(key=lambda x: (-float(x[2]), -x[1], x[0]))
    if small:
        lines.append(
            "Small frequent spend (review): "
            + ", ".join(f"{m} ({cnt} txns, ${total})" for m, cnt, total in small[:top_n])
        )

    return lines
