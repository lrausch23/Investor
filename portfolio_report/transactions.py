from __future__ import annotations

import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_report.util import parse_date, parse_money, sniff_delimiter, uniq_sorted


@dataclass(frozen=True)
class NormalizedTransaction:
    date: dt.date
    symbol: str | None
    tx_type: str
    qty: float | None
    price: float | None
    amount: float | None
    fees: float | None
    account: str | None
    description: str | None

    # Cash impacts (conventions):
    # - cash_impact_portfolio: + means cash into account, - means cash out of account
    cash_impact_portfolio: float | None

    # External flow classification:
    # - is_external: True for deposits/withdrawals/taxes paid out of the account
    is_external: bool

    # Investor-perspective external cashflow:
    # - deposit into portfolio => negative
    # - withdrawal/tax out => positive
    external_cashflow_investor: float | None


def _norm_key(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in (s or "")).strip("_")


def _pick(row: dict[str, Any], keys: list[str]) -> Any:
    norm = {_norm_key(k): k for k in row.keys() if k}
    for k in keys:
        if k in norm:
            return row.get(norm[k])
    return None


def _classify_type(raw: str | None) -> str:
    t = (raw or "").strip().upper()
    t = t.replace(" ", "_")
    # Common synonyms.
    if t in {"BUY", "B"}:
        return "BUY"
    if t in {"SELL", "S"}:
        return "SELL"
    if t in {"DIV", "DIVIDEND"}:
        return "DIV"
    if t in {"INT", "INTEREST"}:
        return "INT"
    if t in {"TAX", "WITHHOLDING", "WHT"}:
        return "TAX"
    if t in {"BNK", "BANK", "CASH", "CASH_FLOW", "WITHDRAW", "WITHDRAWAL", "DEPOSIT", "TRANSFER"}:
        return "BNK"
    if t in {"FEE", "COMMISSION"}:
        return "FEE"
    return t or "UNKNOWN"


def load_transactions(path: Path) -> tuple[list[NormalizedTransaction], list[str]]:
    """
    Parse a broker export `transactions.csv` into a normalized schema.

    Emits warnings for ambiguous sign conventions or missing columns.
    """
    warnings: list[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    delim = sniff_delimiter(text)
    reader = csv.DictReader(text.splitlines(), delimiter=delim)

    out: list[NormalizedTransaction] = []
    for row in reader:
        if not row:
            continue
        d = parse_date(_pick(row, ["date", "trade_date", "as_of", "posted_date"]))
        if d is None:
            continue
        symbol_raw = _pick(row, ["symbol", "ticker", "security", "cusip"])
        symbol = str(symbol_raw).strip().upper() if symbol_raw is not None and str(symbol_raw).strip() else None

        tx_type = _classify_type(
            _pick(row, ["action", "type", "transaction_type", "activity", "description_type"])
        )
        qty = parse_money(_pick(row, ["quantity", "qty", "shares"]))
        price = parse_money(_pick(row, ["price", "trade_price"]))
        amount = parse_money(_pick(row, ["amount", "net_amount", "value", "proceeds"]))
        fees = parse_money(_pick(row, ["fees", "commission"]))
        account = _pick(row, ["account", "account_name", "acct"])
        account_s = str(account).strip() if account is not None and str(account).strip() else None
        desc = _pick(row, ["description", "memo", "details"])
        desc_s = str(desc).strip() if desc is not None and str(desc).strip() else None

        # Best-effort cash impact (portfolio perspective).
        # If `amount` is present, assume it already reflects cash impact (common exports):
        # - BUY negative, SELL positive, DIV positive, TAX negative, fees negative.
        cash_impact = amount
        if cash_impact is None and qty is not None and price is not None:
            gross = float(qty) * float(price)
            if tx_type == "BUY":
                cash_impact = -abs(gross)
            elif tx_type == "SELL":
                cash_impact = abs(gross)
            else:
                cash_impact = gross
        if cash_impact is not None and fees is not None and fees != 0:
            # Fees typically reduce cash (treat positive fee as cash out).
            cash_impact = float(cash_impact) - abs(float(fees))

        # External classification.
        # `BNK` and `TAX` represent external flows; everything else is internal.
        is_external = tx_type in {"BNK", "TAX"}
        ext_cf_inv = None
        if is_external and cash_impact is not None:
            # Portfolio cash increases => investor deposit (negative cashflow to investor).
            ext_cf_inv = -float(cash_impact)

        out.append(
            NormalizedTransaction(
                date=d,
                symbol=symbol,
                tx_type=tx_type,
                qty=qty,
                price=price,
                amount=amount,
                fees=fees,
                account=account_s,
                description=desc_s,
                cash_impact_portfolio=cash_impact,
                is_external=is_external,
                external_cashflow_investor=ext_cf_inv,
            )
        )

    out.sort(key=lambda t: (t.date, t.symbol or "", t.tx_type))
    if not out:
        warnings.append("No transactions parsed (check delimiter/headers).")

    symbols = uniq_sorted([t.symbol for t in out if t.symbol])
    if not symbols:
        warnings.append("No symbols found in transactions (symbol column missing or empty).")
    return out, warnings


def transactions_by_symbol(txs: list[NormalizedTransaction]) -> dict[str, list[NormalizedTransaction]]:
    out: dict[str, list[NormalizedTransaction]] = {}
    for t in txs:
        if not t.symbol:
            continue
        out.setdefault(t.symbol, []).append(t)
    for sym in out:
        out[sym].sort(key=lambda t: t.date)
    return out
