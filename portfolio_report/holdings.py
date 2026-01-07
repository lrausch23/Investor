from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_report.util import parse_money, sniff_delimiter, uniq_sorted


@dataclass(frozen=True)
class Holding:
    symbol: str
    quantity: float
    market_value: float | None
    cost_basis: float | None


def _norm_key(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in (s or "")).strip("_")


def _pick(row: dict[str, Any], keys: list[str]) -> Any:
    norm = {_norm_key(k): k for k in row.keys() if k}
    for k in keys:
        if k in norm:
            return row.get(norm[k])
    return None


def load_holdings(path: Path) -> tuple[list[Holding], list[str]]:
    warnings: list[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    delim = sniff_delimiter(text)
    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    out: list[Holding] = []
    for row in reader:
        if not row:
            continue
        sym = _pick(row, ["symbol", "ticker", "security"])
        if sym is None or not str(sym).strip():
            continue
        symbol = str(sym).strip().upper()
        qty = parse_money(_pick(row, ["quantity", "qty", "shares"]))
        if qty is None:
            continue
        mv = parse_money(_pick(row, ["market_value", "value", "mv"]))
        cb = parse_money(_pick(row, ["cost_basis", "basis", "cost"]))
        out.append(Holding(symbol=symbol, quantity=float(qty), market_value=(float(mv) if mv is not None else None), cost_basis=(float(cb) if cb is not None else None)))
    out.sort(key=lambda h: h.symbol)
    if not out:
        warnings.append("No holdings parsed (check headers).")
    syms = uniq_sorted([h.symbol for h in out])
    if len(syms) != len(out):
        warnings.append("Duplicate symbols in holdings; values may be aggregated incorrectly.")
    return out, warnings

