from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from .store import DEFAULT_SHARADAR_DIR, SharadarStore


QUALITY_FIELDS: tuple[str, ...] = (
    "netinc",
    "revenue",
    "assets",
    "liabilities",
    "ebit",
    "taxexp",
    "debt",
    "equity",
    "assetsc",
    "liabilitiesc",
)


@dataclass(frozen=True)
class PITQualitySignal:
    """Point-in-time quality gate output.

    UNAVAILABLE is intentionally fail-closed. Campaign code can still record
    why it failed, but it must not silently fall back to current fundamentals.
    """

    status: str
    quality_gate_pass: bool
    quality_score: float | None
    reason: str
    fields: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SharadarFundamentalsProvider:
    """As-reported SF1 fundamentals keyed by permaticker/datekey."""

    def __init__(self, store: SharadarStore | str | Path = DEFAULT_SHARADAR_DIR) -> None:
        self.store = store if isinstance(store, SharadarStore) else SharadarStore(store)

    def quality_for_ticker(self, ticker: str, as_of_date: str | pd.Timestamp) -> PITQualitySignal:
        resolution = self.store.resolve_ticker(ticker, as_of_date=as_of_date)
        if resolution is None:
            return PITQualitySignal("UNAVAILABLE", False, None, "ticker_not_resolved", {})
        return self.quality_for_permaticker(resolution.permaticker, as_of_date)

    def quality_for_permaticker(self, permaticker: int, as_of_date: str | pd.Timestamp) -> PITQualitySignal:
        fields = self.store.get_fundamentals_asof(permaticker, as_of_date, QUALITY_FIELDS)
        if not fields:
            return PITQualitySignal("UNAVAILABLE", False, None, "sf1_unavailable", {})
        return _quality_signal_from_fields(fields)

    def quality_series_for_permaticker(
        self,
        permaticker: int,
        dates: Sequence[str | pd.Timestamp],
    ) -> list[PITQualitySignal]:
        date_values = [pd.Timestamp(date) for date in dates]
        if not date_values:
            return []
        history = self.store.get_fundamentals_history(permaticker, max(date_values), QUALITY_FIELDS)
        if history.empty:
            return [PITQualitySignal("UNAVAILABLE", False, None, "sf1_unavailable", {}) for _ in date_values]
        rows = history.copy()
        if "dimension" in rows.columns:
            dim_order = {"ARQ": 0, "ART": 1, "ARY": 2}
            rows["_dimension_order"] = rows["dimension"].astype(str).str.upper().map(dim_order).fillna(999)
        else:
            rows["_dimension_order"] = 999
        signals: list[PITQualitySignal] = []
        for date in date_values:
            eligible = rows.loc[rows["datekey"] <= date].copy()
            if eligible.empty:
                signals.append(PITQualitySignal("UNAVAILABLE", False, None, "sf1_unavailable", {}))
                continue
            row = eligible.sort_values(["datekey", "_dimension_order"], ascending=[False, True]).iloc[0]
            fields = {
                "permaticker": int(permaticker),
                "datekey": pd.Timestamp(row["datekey"]).date().isoformat(),
                "dimension": str(row.get("dimension") or ""),
            }
            for field in QUALITY_FIELDS:
                fields[field] = row.get(field)
            signals.append(_quality_signal_from_fields(fields))
        return signals


def _quality_signal_from_fields(fields: dict[str, Any]) -> PITQualitySignal:
    score, reasons = _quality_score(fields)
    if score is None:
        return PITQualitySignal("UNAVAILABLE", False, None, "insufficient_quality_fields", fields)
    passed = score >= 0.50
    return PITQualitySignal(
        status="PASS" if passed else "FAIL",
        quality_gate_pass=passed,
        quality_score=score,
        reason=";".join(reasons) if reasons else "quality_score",
        fields=fields,
    )


class SharadarFrameLoader:
    """FrameLoader-compatible adapter for campaign backtests."""

    def __init__(
        self,
        store: SharadarStore | str | Path = DEFAULT_SHARADAR_DIR,
        *,
        fundamentals_provider: SharadarFundamentalsProvider | None = None,
    ) -> None:
        self.store = store if isinstance(store, SharadarStore) else SharadarStore(store)
        self.fundamentals_provider = fundamentals_provider or SharadarFundamentalsProvider(self.store)

    def __call__(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        resolution = self.store.resolve_ticker(ticker, as_of_date=start_ts)
        if resolution is None:
            resolution = self.store.resolve_ticker(ticker, as_of_date=end_ts)
        if resolution is None:
            return pd.DataFrame()
        prices = self.store.get_prices([resolution.permaticker], start=start, end=end).get(resolution.permaticker)
        if prices is None or prices.empty:
            prices = self.store.get_benchmark_prices([ticker], start=start, end=end).get(str(ticker).upper())
        if prices is None or prices.empty:
            return pd.DataFrame()
        frame = prices.copy()
        frame.index = pd.to_datetime(frame.index)
        frame = frame.loc[(frame.index >= start_ts) & (frame.index <= end_ts)].copy()
        if frame.empty:
            return frame
        frame["ticker"] = str(ticker).upper()
        frame["permaticker"] = int(resolution.permaticker)
        signals = self.fundamentals_provider.quality_series_for_permaticker(resolution.permaticker, list(frame.index))
        frame["quality_signal_status"] = [signal.status for signal in signals]
        frame["quality_gate_pass"] = [bool(signal.quality_gate_pass) for signal in signals]
        frame["quality_score"] = [signal.quality_score for signal in signals]
        frame["quality_reason"] = [signal.reason for signal in signals]
        frame["data_source"] = "sharadar"
        return frame.sort_index()

    def snapshot_metadata(self) -> dict[str, Any]:
        manifest = self.store.manifest()
        return {
            "data_source": "sharadar",
            "data_snapshot_hash": manifest.get("data_snapshot_hash"),
            "manifest_schema": manifest.get("schema"),
            "downloaded_at": manifest.get("downloaded_at"),
        }


def _quality_score(fields: dict[str, Any]) -> tuple[float | None, list[str]]:
    checks: list[bool] = []
    reasons: list[str] = []

    def add(name: str, passed: bool | None) -> None:
        if passed is None:
            return
        checks.append(bool(passed))
        reasons.append(f"{name}={'pass' if passed else 'fail'}")

    netinc = _num(fields.get("netinc"))
    revenue = _num(fields.get("revenue"))
    assets = _num(fields.get("assets"))
    liabilities = _num(fields.get("liabilities"))
    ebit = _num(fields.get("ebit"))
    taxexp = _num(fields.get("taxexp"))
    debt = _num(fields.get("debt"))
    equity = _num(fields.get("equity"))
    assetsc = _num(fields.get("assetsc"))
    liabilitiesc = _num(fields.get("liabilitiesc"))

    add("profitability", None if netinc is None else netinc > 0)
    add("revenue", None if revenue is None else revenue > 0)
    add("solvency", None if assets is None or liabilities is None else assets > liabilities)
    add("operating_income", None if ebit is None else ebit > 0)
    invested_capital = (debt or 0.0) + (equity or 0.0)
    if ebit is not None and invested_capital > 0:
        nopat = ebit - (taxexp or 0.0)
        add("roic_positive", nopat / invested_capital > 0)
    if assetsc is None or liabilitiesc is None or liabilitiesc == 0.0:
        add("current_ratio", None)
    else:
        add("current_ratio", assetsc / liabilitiesc > 1.0)

    if not checks:
        return None, reasons
    return sum(1.0 for passed in checks if passed) / len(checks), reasons


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed
