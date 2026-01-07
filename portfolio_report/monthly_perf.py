from __future__ import annotations

import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_report.util import last_day_of_month, parse_date, parse_money, sniff_delimiter


@dataclass(frozen=True)
class MonthlyPerfRow:
    month_end: dt.date
    begin_value: float | None
    end_value: float | None
    contributions: float  # >= 0, portfolio perspective
    withdrawals: float  # >= 0, portfolio perspective
    taxes_withheld: float  # >= 0, portfolio perspective
    fees: float  # >= 0, portfolio perspective
    income: float  # >= 0
    warnings: list[str]

    @property
    def net_external_flow_portfolio(self) -> float:
        # Contributions add to portfolio, withdrawals/taxes remove.
        return float(self.contributions) - float(self.withdrawals) - float(self.taxes_withheld)


def _norm_key(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in (s or "")).strip("_")


def _pick(row: dict[str, Any], keys: list[str]) -> Any:
    norm = {_norm_key(k): k for k in row.keys() if k}
    for k in keys:
        if k in norm:
            return row.get(norm[k])
    return None


def load_monthly_perf(path: Path, *, start: dt.date, end: dt.date) -> tuple[list[MonthlyPerfRow], list[str]]:
    """
    Parse `monthly_perf_csv` into month-end begin/end values and (net) external flows.

    This parser is intentionally tolerant:
    - it aggregates rows into month buckets
    - it prefers the latest dated row in a month for end value
    - it treats Contributions/Withdrawals as magnitudes (>=0) regardless of sign formatting
    """
    warnings: list[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    delim = sniff_delimiter(text)
    reader = csv.DictReader(text.splitlines(), delimiter=delim)

    buckets: dict[dt.date, dict[str, Any]] = {}
    for row in reader:
        if not row:
            continue
        d = parse_date(_pick(row, ["date", "as_of", "period_end", "month_end"]))
        if d is None:
            continue
        me = last_day_of_month(d)
        if me < start or me > end:
            continue

        b = buckets.setdefault(
            me,
            {
                "month_end": me,
                "min_date": d,
                "max_date": d,
                "begin_value": None,
                "end_value": None,
                "contributions": 0.0,
                "withdrawals": 0.0,
                "taxes": 0.0,
                "fees": 0.0,
                "income": 0.0,
                "row_count": 0,
            },
        )
        b["row_count"] += 1
        if d < b["min_date"]:
            b["min_date"] = d
        if d > b["max_date"]:
            b["max_date"] = d

        begin_v = parse_money(
            _pick(row, ["begin_value", "beginning_market_value", "beginning_value", "beginning_market_value_net_contributions_withdrawals"])
        )
        end_v = parse_money(_pick(row, ["end_value", "ending_market_value", "ending_value", "market_value_end"]))
        contrib = parse_money(_pick(row, ["contributions", "contribution", "deposits", "deposit"]))
        withdraw = parse_money(_pick(row, ["withdrawals", "withdrawal", "distributions", "distribution"]))
        income = parse_money(_pick(row, ["income", "dividends", "interest"]))
        fees = parse_money(_pick(row, ["fees", "fee"]))
        taxes = parse_money(_pick(row, ["tax", "taxes", "withholding", "taxes_withheld"]))

        if begin_v is not None:
            # Prefer the earliest dated row for begin.
            if b["begin_value"] is None or d <= b["min_date"]:
                b["begin_value"] = float(begin_v)
        if end_v is not None:
            # Prefer the latest dated row for end.
            if b["end_value"] is None or d >= b["max_date"]:
                b["end_value"] = float(end_v)

        # Treat flows as magnitudes. Negative formatting is common ("(18,000)").
        if contrib is not None:
            b["contributions"] += abs(float(contrib))
        if withdraw is not None:
            b["withdrawals"] += abs(float(withdraw))
        if taxes is not None:
            b["taxes"] += abs(float(taxes))
        if fees is not None:
            b["fees"] += abs(float(fees))
        if income is not None:
            b["income"] += abs(float(income))

    rows: list[MonthlyPerfRow] = []
    for me in sorted(buckets.keys()):
        b = buckets[me]
        row_warn: list[str] = []
        if b["begin_value"] is None:
            row_warn.append("Missing begin value for month.")
        if b["end_value"] is None:
            row_warn.append("Missing end value for month.")
        rows.append(
            MonthlyPerfRow(
                month_end=me,
                begin_value=b["begin_value"],
                end_value=b["end_value"],
                contributions=float(b["contributions"]),
                withdrawals=float(b["withdrawals"]),
                taxes_withheld=float(b["taxes"]),
                fees=float(b["fees"]),
                income=float(b["income"]),
                warnings=row_warn,
            )
        )

    if not rows:
        warnings.append("No monthly performance rows parsed (check headers and date column).")
    return rows, warnings

