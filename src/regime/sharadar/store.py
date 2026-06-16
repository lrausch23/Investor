from __future__ import annotations

import json
import hashlib
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

DEFAULT_SHARADAR_DIR = Path("data") / "sharadar"
FACT_TABLES = {"SEP", "SFP", "SF1", "DAILY"}
META_TABLES = {"TICKERS", "ACTIONS", "SP500"}
AS_REPORTED_DIMENSIONS = ("ARQ", "ART", "ARY")
TERMINAL_DEFAULT_POLICY_VERSION = "terminal_value_policy.v3.reason_dependent_terminal_values"
TERMINAL_DEFAULTS_ARTIFACT_NAME = "terminal_value_defaults.json"


@dataclass(frozen=True)
class TickerResolution:
    ticker: str
    permaticker: int
    start_date: pd.Timestamp | None = None
    end_date: pd.Timestamp | None = None


@dataclass(frozen=True)
class TerminalValueEvent:
    permaticker: int
    date: pd.Timestamp
    value: float
    source: str
    reason: str
    requires_human_review: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "permaticker": int(self.permaticker),
            "date": pd.Timestamp(self.date).date().isoformat(),
            "value": float(self.value),
            "source": self.source,
            "reason": self.reason,
            "requires_human_review": bool(self.requires_human_review),
        }


SEEDED_TERMINAL_EVENTS: dict[int, dict[str, Any]] = {
    119243: {
        "date": "2023-03-12",
        "value": 0.0,
        "source": "seeded_failure_default_zero",
        "reason": "Signature Bank FDIC receivership; common equity wiped out.",
    }
}

TERMINAL_FAILURE_PATTERN = "bankrupt|bankruptcy|receivership|liquidation|failure|failed|fdic"
TERMINAL_ACTION_PATTERN = (
    "delist|delete|bankrupt|bankruptcy|receivership|liquidat|failure|failed|fdic|"
    "chapter 7|chapter 11|\\bacquisition\\b|acquisitionby|acquisition by|cash acquisition|cash buyout|buyout|going private|merger"
)
TERMINAL_ACQUISITION_LIKE_PATTERN = "acquisition|acquisitionby|buyout|going private|going-private|merger|takeover|cash"


class SharadarStore:
    """Local point-in-time Sharadar store keyed by permaticker."""

    def __init__(self, root: str | Path = DEFAULT_SHARADAR_DIR) -> None:
        self.root = Path(root)
        self.facts_dir = self.root / "facts"
        self.sqlite_path = self.root / "metadata.sqlite"
        self.manifest_path = self.root / "manifest.json"
        self._meta_cache: dict[str, pd.DataFrame] = {}

    def exists(self) -> bool:
        return self.manifest_path.exists() and self.sqlite_path.exists()

    def manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return {}
        return dict(json.loads(self.manifest_path.read_text(encoding="utf-8")))

    @property
    def data_snapshot_hash(self) -> str | None:
        value = self.manifest().get("data_snapshot_hash")
        return str(value) if value else None

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.sqlite_path)

    def read_fact_table(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
    ) -> pd.DataFrame:
        normalized = _table_name(table)
        path = self.facts_dir / normalized
        if path.is_dir():
            files = sorted(path.glob("*.parquet"))
            if not files:
                return pd.DataFrame()
            frames = [
                frame
                for frame in (_read_parquet(file, columns=columns, filters=filters) for file in files)
                if not frame.empty
            ]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        file_path = self.facts_dir / f"{normalized}.parquet"
        if file_path.exists():
            return _read_parquet(file_path, columns=columns, filters=filters)
        return pd.DataFrame()

    def read_meta_table(self, table: str) -> pd.DataFrame:
        normalized = _table_name(table)
        cached = self._meta_cache.get(normalized)
        if cached is not None:
            return cached.copy()
        if not self.sqlite_path.exists():
            return pd.DataFrame()
        with self.connect() as conn:
            try:
                frame = pd.read_sql_query(f'SELECT * FROM "{normalized}"', conn)
            except Exception:
                return pd.DataFrame()
        self._meta_cache[normalized] = frame
        return frame.copy()

    def resolve_ticker(self, ticker: str, as_of_date: str | pd.Timestamp | None = None) -> TickerResolution | None:
        symbol = _normalize_ticker(ticker)
        if not symbol:
            return None
        tickers = self.read_meta_table("TICKERS")
        if tickers.empty or "permaticker" not in tickers.columns:
            return None
        if symbol.isdigit():
            rows = tickers.loc[pd.to_numeric(tickers["permaticker"], errors="coerce") == int(symbol)].copy()
        else:
            ticker_column = _first_existing(tickers, ["ticker", "symbol"])
            if ticker_column is None:
                return None
            rows = tickers.loc[tickers[ticker_column].astype(str).str.upper() == symbol].copy()
        if rows.empty:
            return None
        as_of = pd.Timestamp(as_of_date).normalize() if as_of_date is not None else None
        if as_of is not None:
            rows["_start"] = _date_series(rows, ["firstpricedate", "listingdate", "startdate", "date"])
            rows["_end"] = _date_series(rows, ["lastpricedate", "delistingdate", "enddate"])
            active = rows.loc[
                (rows["_start"].isna() | (rows["_start"] <= as_of))
                & (rows["_end"].isna() | (rows["_end"] >= as_of))
            ]
            if not active.empty:
                rows = active
        rows["_sort_start"] = _date_series(rows, ["firstpricedate", "listingdate", "startdate", "date"]).fillna(pd.Timestamp.min)
        row = rows.sort_values("_sort_start").iloc[-1]
        start = row.get("_start") if "_start" in row else None
        end = row.get("_end") if "_end" in row else None
        return TickerResolution(
            ticker=symbol,
            permaticker=int(row["permaticker"]),
            start_date=pd.Timestamp(start).normalize() if pd.notna(start) else None,
            end_date=pd.Timestamp(end).normalize() if pd.notna(end) else None,
        )

    def resolve_permatickers(self, identifiers: Iterable[str | int], as_of_date: str | pd.Timestamp | None = None) -> list[int]:
        out: list[int] = []
        seen: set[int] = set()
        for identifier in identifiers:
            value = str(identifier).strip()
            resolution = self.resolve_ticker(value, as_of_date=as_of_date)
            permaticker = resolution.permaticker if resolution else _parse_int(value)
            if permaticker is not None and permaticker not in seen:
                seen.add(permaticker)
                out.append(permaticker)
        return out

    def ticker_for_permaticker(self, permaticker: int, as_of_date: str | pd.Timestamp | None = None) -> str:
        tickers = self.read_meta_table("TICKERS")
        if tickers.empty or "permaticker" not in tickers.columns:
            return str(permaticker)
        rows = tickers.loc[pd.to_numeric(tickers["permaticker"], errors="coerce") == int(permaticker)].copy()
        if rows.empty:
            return str(permaticker)
        as_of = pd.Timestamp(as_of_date).normalize() if as_of_date is not None else None
        column = _first_existing(rows, ["ticker", "symbol"])
        if column is not None:
            non_null = rows.loc[
                rows[column].notna()
                & ~rows[column].astype(str).str.strip().str.upper().isin({"", "NONE", "NAN"})
            ].copy()
            if not non_null.empty:
                rows = non_null
        if "table" in rows.columns:
            preferred = rows.loc[rows["table"].astype(str).str.upper().isin({"SEP", "SF1"})].copy()
            if not preferred.empty:
                rows = preferred
        if as_of is not None:
            rows["_start"] = _date_series(rows, ["firstpricedate", "listingdate", "startdate", "date"])
            rows["_end"] = _date_series(rows, ["lastpricedate", "delistingdate", "enddate"])
            active = rows.loc[
                (rows["_start"].isna() | (rows["_start"] <= as_of))
                & (rows["_end"].isna() | (rows["_end"] >= as_of))
            ]
            if not active.empty:
                rows = active
        return str(rows.iloc[-1].get(column) if column else permaticker).upper()

    def get_prices(self, permatickers: Sequence[int | str], start: str, end: str) -> dict[int, pd.DataFrame]:
        return self._get_adjusted_prices("SEP", permatickers, start, end)

    def terminal_value_events(
        self,
        permatickers: Sequence[int | str],
        *,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> dict[int, TerminalValueEvent]:
        wanted = {perma for perma in (_parse_security_permaticker(item) for item in permatickers) if perma is not None}
        if not wanted:
            return {}
        start_ts = pd.Timestamp(start).normalize() if start is not None else None
        end_ts = pd.Timestamp(end).normalize() if end is not None else None
        events: dict[int, TerminalValueEvent] = {}
        details = self._terminal_candidate_details(wanted, end=end_ts)
        terminal_dates = {
            int(perma): pd.Timestamp(detail.get("date") or end_ts or pd.Timestamp.today()).normalize()
            for perma, detail in details.items()
        }
        last_prices = self._terminal_last_price_info(terminal_dates)
        health = self._terminal_fundamental_health(terminal_dates)
        for perma, payload in SEEDED_TERMINAL_EVENTS.items():
            if perma in wanted:
                seeded = TerminalValueEvent(
                    permaticker=perma,
                    date=pd.Timestamp(payload["date"]).normalize(),
                    value=float(payload["value"]),
                    source=str(payload["source"]),
                    reason=str(payload["reason"]),
                )
                current = events.get(perma)
                if current is None or seeded.date <= current.date:
                    events[perma] = seeded
        for perma, detail in sorted(details.items()):
            if perma in events:
                continue
            events[perma] = self._terminal_event_from_detail(
                detail,
                last_prices.get(int(perma), {}),
                health.get(int(perma), {}),
                end_ts=end_ts,
            )
        if start_ts is not None:
            events = {perma: event for perma, event in events.items() if event.date >= start_ts}
        if end_ts is not None:
            events = {perma: event for perma, event in events.items() if event.date <= end_ts}
        return events

    def delisted_permatickers(self, permatickers: Sequence[int | str], *, end: str | pd.Timestamp) -> set[int]:
        wanted = {perma for perma in (_parse_security_permaticker(item) for item in permatickers) if perma is not None}
        if not wanted:
            return set()
        end_ts = pd.Timestamp(end).normalize()
        return {perma for perma, detail in self._terminal_candidate_details(wanted, end=end_ts).items() if pd.Timestamp(detail["date"]).normalize() <= end_ts}

    def refresh_terminal_value_defaults_artifact(self, *, end: str | pd.Timestamp | None = None) -> dict[str, Any]:
        """Persist reason-dependent terminal values and stamp them into the snapshot hash."""

        end_ts = pd.Timestamp(end).normalize() if end is not None else None
        candidates = sorted(self._terminal_candidate_details(None, end=end_ts).keys())
        events = self.terminal_value_events(candidates, end=end_ts) if candidates else {}
        ticker_lookup = self._ticker_labels_for_permatickers(candidates)
        rows = []
        for perma in candidates:
            event = events.get(perma)
            if event is None:
                continue
            row = event.to_dict()
            row["ticker"] = ticker_lookup.get(perma) or str(perma)
            row["policy_version"] = TERMINAL_DEFAULT_POLICY_VERSION
            rows.append(row)
        rows = sorted(rows, key=lambda row: (str(row.get("ticker") or ""), int(row.get("permaticker") or 0)))
        category_breakdown = _terminal_category_breakdown(rows)
        stable_payload = {
            "schema": "regime_sharadar_terminal_value_defaults.v1",
            "policy_version": TERMINAL_DEFAULT_POLICY_VERSION,
            "rows": rows,
            "category_breakdown": category_breakdown,
            "production_defaults_changed": False,
        }
        artifact_hash = hashlib.sha256(json.dumps(stable_payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        artifact = {
            **stable_payload,
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "artifact_hash": artifact_hash,
            "row_count": len(rows),
            "conservative_default_count": int(category_breakdown.get("conservative_default_zero", 0)),
            "failure_zero_count": int(
                category_breakdown.get("actions_failure_default_zero", 0)
                + category_breakdown.get("seeded_failure_default_zero", 0)
                + category_breakdown.get("unknown_distressed_zero", 0)
                + category_breakdown.get("unknown_missing_price_zero", 0)
                + category_breakdown.get("acquisition_missing_price_zero", 0)
            ),
            "last_price_terminal_count": int(
                category_breakdown.get("acquisition_last_price", 0)
                + category_breakdown.get("unknown_healthy_last_price", 0)
            ),
            "unknown_residual_count": int(
                category_breakdown.get("unknown_healthy_last_price", 0)
                + category_breakdown.get("unknown_distressed_zero", 0)
                + category_breakdown.get("unknown_missing_price_zero", 0)
            ),
        }
        path = self.root / TERMINAL_DEFAULTS_ARTIFACT_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(artifact, indent=2, sort_keys=True, default=str), encoding="utf-8")
        self._stamp_terminal_defaults_in_manifest(path, artifact_hash, artifact)
        return artifact

    def _stamp_terminal_defaults_in_manifest(self, path: Path, artifact_hash: str, artifact: dict[str, Any]) -> None:
        manifest = self.manifest()
        if not manifest:
            return
        base_hash = str(manifest.get("base_data_snapshot_hash") or manifest.get("data_snapshot_hash") or "")
        manifest["base_data_snapshot_hash"] = base_hash
        manifest["terminal_value_defaults"] = {
            "policy_version": TERMINAL_DEFAULT_POLICY_VERSION,
            "artifact": str(path.relative_to(self.root)),
            "artifact_hash": artifact_hash,
            "row_count": int(artifact.get("row_count") or 0),
            "category_breakdown": dict(artifact.get("category_breakdown") or {}),
            "conservative_default_count": int(artifact.get("conservative_default_count") or 0),
            "failure_zero_count": int(artifact.get("failure_zero_count") or 0),
            "last_price_terminal_count": int(artifact.get("last_price_terminal_count") or 0),
            "unknown_residual_count": int(artifact.get("unknown_residual_count") or 0),
        }
        snapshot_payload = {
            "base_data_snapshot_hash": base_hash,
            "terminal_value_defaults_hash": artifact_hash,
            "terminal_value_policy_version": TERMINAL_DEFAULT_POLICY_VERSION,
        }
        manifest["data_snapshot_hash"] = hashlib.sha256(json.dumps(snapshot_payload, sort_keys=True).encode("utf-8")).hexdigest()
        self.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str), encoding="utf-8")
        self._meta_cache.clear()

    def _terminal_event_from_detail(
        self,
        detail: dict[str, Any],
        last_price: dict[str, Any],
        health: dict[str, Any],
        *,
        end_ts: pd.Timestamp | None,
    ) -> TerminalValueEvent:
        perma = int(detail["permaticker"])
        date = pd.Timestamp(detail.get("date") or end_ts or pd.Timestamp.today()).normalize()
        reason = str(detail.get("reason") or "terminal value unresolved")
        detail_source = str(detail.get("source") or "")
        price = _parse_float(last_price.get("last_price"))
        price_date = last_price.get("last_price_date")
        price_date_text = pd.Timestamp(price_date).date().isoformat() if price_date is not None and pd.notna(price_date) else "unavailable"
        is_failure = _matches(reason, TERMINAL_FAILURE_PATTERN)
        is_acquisition = _matches(reason, TERMINAL_ACQUISITION_LIKE_PATTERN)
        if is_failure:
            return TerminalValueEvent(
                permaticker=perma,
                date=date,
                value=0.0,
                source="actions_failure_default_zero" if detail_source == "ACTIONS" else "metadata_failure_default_zero",
                reason=f"{reason}; failure/receivership/liquidation terminal value set to $0 under {TERMINAL_DEFAULT_POLICY_VERSION}",
            )
        if is_acquisition:
            if price is not None and price > 0:
                return TerminalValueEvent(
                    permaticker=perma,
                    date=date,
                    value=float(price),
                    source="acquisition_last_price",
                    reason=f"{reason}; acquisition/merger terminal value uses last SEP adjusted price from {price_date_text} under {TERMINAL_DEFAULT_POLICY_VERSION}",
                )
            return TerminalValueEvent(
                permaticker=perma,
                date=date,
                value=0.0,
                source="acquisition_missing_price_zero",
                reason=f"{reason}; acquisition-like terminal event lacked a usable prior SEP price, so value defaulted to $0 under {TERMINAL_DEFAULT_POLICY_VERSION}",
                requires_human_review=True,
            )

        branch = _unknown_terminal_health_branch(last_price, health)
        branch_reason = _unknown_terminal_branch_reason(last_price, health)
        if branch == "healthy_last_price" and price is not None and price > 0:
            return TerminalValueEvent(
                permaticker=perma,
                date=date,
                value=float(price),
                source="unknown_healthy_last_price",
                reason=f"{reason}; unknown delisting resolved as healthy/likely acquisition using last SEP adjusted price from {price_date_text}; {branch_reason}",
                requires_human_review=True,
            )
        source = "unknown_missing_price_zero" if price is None else "unknown_distressed_zero"
        return TerminalValueEvent(
            permaticker=perma,
            date=date,
            value=0.0,
            source=source,
            reason=f"{reason}; unknown delisting resolved as distressed/failure with $0 terminal value; {branch_reason}",
            requires_human_review=True,
        )

    def _terminal_last_price_info(self, terminal_dates: dict[int, pd.Timestamp]) -> dict[int, dict[str, Any]]:
        wanted = sorted({int(perma) for perma in terminal_dates})
        if not wanted:
            return {}
        sep = self.read_fact_table(
            "SEP",
            columns=["permaticker", "date", "close", "closeadj"],
            filters=[("permaticker", "in", wanted)],
        )
        if sep.empty or "permaticker" not in sep.columns or "date" not in sep.columns:
            return {}
        rows = _normalize_columns(sep)
        rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
        rows["date"] = pd.to_datetime(rows["date"], errors="coerce").dt.normalize()
        close_col = _first_existing(rows, ["close", "closeunadj", "price"])
        adj_col = _first_existing(rows, ["closeadj", "adjclose", "adj_close"])
        if close_col is None:
            return {}
        price = pd.to_numeric(rows[adj_col], errors="coerce") if adj_col is not None else pd.to_numeric(rows[close_col], errors="coerce")
        rows["_price"] = price
        rows = rows.dropna(subset=["permaticker", "date", "_price"]).copy()
        out: dict[int, dict[str, Any]] = {}
        for perma, group in rows.sort_values("date").groupby("permaticker", sort=False):
            terminal_date = pd.Timestamp(terminal_dates.get(int(perma))).normalize()
            history = group.loc[group["date"] <= terminal_date].copy()
            if history.empty:
                continue
            last = history.iloc[-1]
            last_price = float(last["_price"])
            trailing_high = float(pd.to_numeric(history["_price"], errors="coerce").max())
            drawdown_pct = (last_price / trailing_high - 1.0) if trailing_high > 0 else None
            out[int(perma)] = {
                "last_price": last_price,
                "last_price_date": pd.Timestamp(last["date"]).normalize(),
                "trailing_high": trailing_high,
                "drawdown_pct": drawdown_pct,
                "price_collapse": bool(drawdown_pct is not None and drawdown_pct <= -0.70),
            }
        return out

    def _terminal_fundamental_health(self, terminal_dates: dict[int, pd.Timestamp]) -> dict[int, dict[str, Any]]:
        wanted = sorted({int(perma) for perma in terminal_dates})
        if not wanted:
            return {}
        sf1 = self.read_fact_table(
            "SF1",
            columns=["permaticker", "datekey", "dimension", "netinc", "equity", "assets", "liabilities"],
            filters=[("permaticker", "in", wanted)],
        )
        if sf1.empty or "permaticker" not in sf1.columns or "datekey" not in sf1.columns:
            return {}
        rows = _normalize_columns(sf1)
        rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
        rows["datekey"] = pd.to_datetime(rows["datekey"], errors="coerce").dt.normalize()
        if "dimension" in rows.columns:
            dim_rank = {dim: idx for idx, dim in enumerate(AS_REPORTED_DIMENSIONS)}
            rows["_dimension_rank"] = rows["dimension"].astype(str).str.upper().map(dim_rank).fillna(999)
        else:
            rows["_dimension_rank"] = 999
        rows = rows.dropna(subset=["permaticker", "datekey"]).copy()
        out: dict[int, dict[str, Any]] = {}
        for perma, group in rows.sort_values(["datekey", "_dimension_rank"]).groupby("permaticker", sort=False):
            terminal_date = pd.Timestamp(terminal_dates.get(int(perma))).normalize()
            history = group.loc[group["datekey"] <= terminal_date].copy()
            if history.empty:
                continue
            row = history.sort_values(["datekey", "_dimension_rank"], ascending=[False, True]).iloc[0]
            equity = _parse_float(row.get("equity"))
            assets = _parse_float(row.get("assets"))
            liabilities = _parse_float(row.get("liabilities"))
            if equity is None and assets is not None and liabilities is not None:
                equity = assets - liabilities
            netinc = _parse_float(row.get("netinc"))
            out[int(perma)] = {
                "datekey": pd.Timestamp(row["datekey"]).normalize(),
                "equity": equity,
                "netinc": netinc,
                "positive_equity": bool(equity is not None and equity > 0),
                "negative_equity": bool(equity is not None and equity < 0),
                "profitable": bool(netinc is not None and netinc > 0),
                "unprofitable": bool(netinc is not None and netinc < 0),
            }
        return out

    def _terminal_candidate_details(
        self,
        permatickers: set[int] | None,
        *,
        end: pd.Timestamp | None,
    ) -> dict[int, dict[str, Any]]:
        wanted = {int(item) for item in permatickers} if permatickers is not None else None
        details: dict[int, dict[str, Any]] = {}
        actions = self.read_meta_table("ACTIONS")
        if not actions.empty and "permaticker" in actions.columns:
            rows = _normalize_columns(actions)
            rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
            date_col = _first_existing(rows, ["date", "actiondate"])
            event_col = _first_existing(rows, ["action", "event", "type"])
            if date_col is not None:
                rows[date_col] = pd.to_datetime(rows[date_col], errors="coerce").dt.normalize()
                rows = rows.dropna(subset=["permaticker", date_col]).copy()
                if wanted is not None:
                    rows = rows.loc[rows["permaticker"].isin(wanted)].copy()
                if end is not None:
                    rows = rows.loc[rows[date_col] <= end].copy()
                if event_col is not None:
                    rows = rows.loc[
                        rows[event_col]
                        .astype(str)
                        .str.lower()
                        .str.contains(TERMINAL_ACTION_PATTERN, regex=True, na=False)
                    ].copy()
                for _, row in rows.sort_values(date_col).iterrows():
                    perma = int(row["permaticker"])
                    reason = str(row.get(event_col) or "terminal corporate action") if event_col is not None else "terminal corporate action"
                    details[perma] = _choose_terminal_detail(
                        details.get(perma),
                        {
                            "permaticker": perma,
                            "date": pd.Timestamp(row[date_col]).normalize(),
                            "reason": reason,
                            "source": "ACTIONS",
                            "requires_human_review": _matches(reason, TERMINAL_ACQUISITION_LIKE_PATTERN),
                        },
                    )
        tickers = self.read_meta_table("TICKERS")
        if not tickers.empty and "permaticker" in tickers.columns:
            rows = _normalize_columns(tickers)
            rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
            if wanted is not None:
                rows = rows.loc[rows["permaticker"].isin(wanted)].copy()
            if not rows.empty:
                is_delisted = rows.get("isdelisted", pd.Series("", index=rows.index)).astype(str).str.upper().eq("Y")
                if "category" in rows.columns:
                    common = rows["category"].astype(str).str.upper().str.contains("COMMON STOCK|ADR|ADS|SHARE", regex=True, na=False)
                    rows = rows.loc[is_delisted & common].copy()
                else:
                    rows = rows.loc[is_delisted].copy()
                end_dates = _date_series(rows, ["lastpricedate", "delistingdate", "enddate"])
                for idx, row in rows.loc[end_dates.notna()].iterrows():
                    date = pd.Timestamp(end_dates.loc[idx]).normalize()
                    if end is not None and date > end:
                        continue
                    parsed_perma = _parse_int(row.get("permaticker"))
                    if parsed_perma is None:
                        continue
                    reason_parts = ["ticker_metadata_isdelisted"]
                    for column in ("delistingreason", "delisting_reason", "sicindustry", "sector"):
                        value = str(row.get(column) or "").strip()
                        if value and value.lower() != "nan":
                            reason_parts.append(value)
                    details[parsed_perma] = _choose_terminal_detail(
                        details.get(parsed_perma),
                        {
                            "permaticker": int(parsed_perma),
                            "date": date,
                            "reason": "; ".join(reason_parts),
                            "source": "TICKERS",
                            "requires_human_review": _matches("; ".join(reason_parts), TERMINAL_ACQUISITION_LIKE_PATTERN),
                        },
                    )
        return details

    def _ticker_labels_for_permatickers(self, permatickers: Sequence[int]) -> dict[int, str]:
        wanted = sorted({int(perma) for perma in permatickers})
        if not wanted:
            return {}
        tickers = self.read_meta_table("TICKERS")
        if tickers.empty or "permaticker" not in tickers.columns:
            return {perma: str(perma) for perma in wanted}
        rows = _normalize_columns(tickers)
        rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
        rows = rows.loc[rows["permaticker"].isin(wanted)].copy()
        if rows.empty:
            return {perma: str(perma) for perma in wanted}
        ticker_col = _first_existing(rows, ["ticker", "symbol"])
        if ticker_col is None:
            return {perma: str(perma) for perma in wanted}
        if "table" in rows.columns:
            rows["_table_rank"] = rows["table"].astype(str).str.upper().map({"SEP": 0, "SF1": 1}).fillna(2)
        else:
            rows["_table_rank"] = 0
        rows["_end"] = _date_series(rows, ["lastpricedate", "delistingdate", "enddate"]).fillna(pd.Timestamp.max)
        out: dict[int, str] = {}
        for perma, group in rows.sort_values(["_table_rank", "_end"]).groupby("permaticker", sort=False):
            values = [
                str(value).strip().upper()
                for value in group[ticker_col].dropna().tolist()
                if str(value).strip() and str(value).strip().lower() != "nan"
            ]
            if values:
                out[int(perma)] = values[-1]
        for perma in wanted:
            out.setdefault(perma, str(perma))
        return out

    def get_benchmark_prices(self, identifiers: Sequence[str | int], start: str, end: str) -> dict[str, pd.DataFrame]:
        """Return adjusted ETF/fund benchmark frames from Sharadar SFP.

        SFP benchmarks are single instruments rather than survivorship-free
        equity universes, but keeping them in the same local Sharadar snapshot
        avoids mixing vendor methodologies inside PIT studies.
        """

        out: dict[str, pd.DataFrame] = {}
        resolved: dict[str, int] = {}
        for identifier in identifiers:
            key = str(identifier).strip().upper()
            if not key:
                continue
            resolution = self.resolve_ticker(key, as_of_date=start)
            if resolution is None:
                resolution = self.resolve_ticker(key, as_of_date=end)
            if resolution is not None:
                resolved[key] = int(resolution.permaticker)
            else:
                perma = _parse_int(key)
                if perma is not None:
                    resolved[key] = perma
        frames = self._get_adjusted_prices("SFP", list(resolved.values()), start, end)
        for key, perma in resolved.items():
            out[key] = frames.get(perma, pd.DataFrame())
        for identifier in identifiers:
            key = str(identifier).strip().upper()
            out.setdefault(key, pd.DataFrame())
        return out

    def sp500_membership_asof(self, date: str | pd.Timestamp) -> list[int]:
        """Return point-in-time S&P 500 members from Sharadar SP500 data."""

        rows = self._sp500_rows()
        if rows.empty:
            return []
        as_of = pd.Timestamp(date).normalize()
        rows = rows.loc[rows["date"] <= as_of].copy()
        if rows.empty:
            return []
        snapshots = rows.loc[rows["action"].isin({"historical", "current"})].copy()
        members: set[int] = set()
        snapshot_date: pd.Timestamp | None = None
        if not snapshots.empty:
            snapshot_date = pd.Timestamp(snapshots["date"].max()).normalize()
            members = set(
                pd.to_numeric(snapshots.loc[snapshots["date"] == snapshot_date, "permaticker"], errors="coerce")
                .dropna()
                .astype(int)
                .tolist()
            )
        events = rows if snapshot_date is None else rows.loc[rows["date"] > snapshot_date].copy()
        for _, row in events.sort_values("date").iterrows():
            perma = _parse_int(row.get("permaticker"))
            if perma is None:
                continue
            action = str(row.get("action") or "").strip().lower()
            if "removed" in action or "delete" in action:
                members.discard(perma)
            elif action in {"added", "historical", "current"} or not action:
                members.add(perma)
        return sorted(members)

    def synth_sp500_total_return(
        self,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        *,
        reconstitution: str = "monthly",
        base_level: float = 100.0,
    ) -> pd.DataFrame:
        """Build a cap-weighted S&P 500 total-return proxy from SP500 + SEP + DAILY.

        The benchmark uses point-in-time SP500 membership, adjusted SEP prices, and
        as-of DAILY market caps. Membership is refreshed monthly by default.
        """

        if str(reconstitution).lower() != "monthly":
            raise ValueError("synth_sp500_total_return currently supports monthly reconstitution only.")
        start_ts = pd.Timestamp(start).normalize()
        end_ts = pd.Timestamp(end).normalize()
        if end_ts < start_ts:
            return pd.DataFrame()
        candidates = self._sp500_member_candidates(start_ts, end_ts)
        if not candidates:
            return pd.DataFrame()
        prices = self.get_prices(candidates, start_ts.date().isoformat(), end_ts.date().isoformat())
        price_series = {
            int(perma): frame["price"].astype(float)
            for perma, frame in prices.items()
            if not frame.empty and "price" in frame.columns
        }
        if not price_series:
            return pd.DataFrame()
        price_panel = pd.concat(price_series, axis=1).sort_index()
        price_panel.index = pd.to_datetime(price_panel.index).normalize()
        price_panel = price_panel.loc[(price_panel.index >= start_ts) & (price_panel.index <= end_ts)]
        price_panel = price_panel[~price_panel.index.duplicated(keep="last")].ffill()
        price_panel = price_panel.dropna(how="all")
        if price_panel.empty:
            return pd.DataFrame()
        date_index = pd.DatetimeIndex(price_panel.index)
        cap_panel = self._marketcap_panel(candidates, start_ts, end_ts, date_index)
        returns = price_panel.pct_change().replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
        rebalance_dates = _monthly_trading_rebalance_dates(date_index, start_ts, end_ts)
        rebalance_set = set(rebalance_dates)
        current_weights = pd.Series(dtype="float64")
        level = float(base_level)
        rows: list[dict[str, Any]] = []
        for idx, date in enumerate(date_index):
            if current_weights.empty or date in rebalance_set:
                current_weights = _synth_weights_for_date(
                    self.sp500_membership_asof(date),
                    date,
                    price_panel=price_panel,
                    cap_panel=cap_panel,
                )
            if current_weights.empty:
                continue
            if idx > 0:
                day_return = returns.loc[date, current_weights.index].fillna(0.0)
                level *= 1.0 + float((day_return * current_weights).sum())
            coverage_columns = [column for column in current_weights.index if not cap_panel.empty and column in cap_panel.columns]
            rows.append(
                {
                    "date": date,
                    "open": level,
                    "high": level,
                    "low": level,
                    "price": level,
                    "volume": 0.0,
                    "member_count": int(len(current_weights)),
                    "marketcap_coverage": float(cap_panel.loc[date, coverage_columns].notna().mean()) if coverage_columns else 0.0,
                    "synthetic_benchmark": "synth_sp500_total_return",
                }
            )
        if not rows:
            return pd.DataFrame()
        out = pd.DataFrame(rows).set_index("date").sort_index()
        return out

    def _sp500_rows(self) -> pd.DataFrame:
        sp500 = self.read_meta_table("SP500")
        if sp500.empty or "permaticker" not in sp500.columns:
            return pd.DataFrame()
        rows = _normalize_columns(sp500)
        if "date" not in rows.columns:
            return pd.DataFrame()
        rows["date"] = pd.to_datetime(rows["date"], errors="coerce").dt.normalize()
        rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
        if "action" in rows.columns:
            rows["action"] = rows["action"].astype(str).str.strip().str.lower()
        else:
            rows["action"] = ""
        return rows.dropna(subset=["date", "permaticker"]).copy()

    def _sp500_member_candidates(self, start: pd.Timestamp, end: pd.Timestamp) -> list[int]:
        rows = self._sp500_rows()
        if rows.empty:
            return []
        rows = rows.loc[rows["date"] <= end].copy()
        if rows.empty:
            return []
        start_snapshot = rows.loc[(rows["date"] <= start) & rows["action"].isin({"historical", "current"})]
        if not start_snapshot.empty:
            first_snapshot_date = pd.Timestamp(start_snapshot["date"].max()).normalize()
            rows = rows.loc[rows["date"] >= first_snapshot_date].copy()
        eligible = rows.loc[~rows["action"].str.contains("removed|delete", regex=True, na=False)].copy()
        values = pd.to_numeric(eligible["permaticker"], errors="coerce").dropna().astype(int).tolist()
        return sorted(set(values))

    def _marketcap_panel(
        self,
        permatickers: Sequence[int],
        start: pd.Timestamp,
        end: pd.Timestamp,
        date_index: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        wanted = sorted({int(perma) for perma in permatickers})
        if not wanted:
            return pd.DataFrame(index=date_index)
        lookback = (start - pd.Timedelta(days=400)).date().isoformat()
        daily = self.read_fact_table(
            "DAILY",
            columns=["permaticker", "date", "marketcap"],
            filters=[
                ("permaticker", "in", wanted),
                ("date", ">=", lookback),
                ("date", "<=", end.date().isoformat()),
            ],
        )
        if daily.empty or "marketcap" not in daily.columns:
            return pd.DataFrame(index=date_index)
        daily = _normalize_columns(daily)
        daily["permaticker"] = pd.to_numeric(daily["permaticker"], errors="coerce")
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
        daily["marketcap"] = pd.to_numeric(daily["marketcap"], errors="coerce")
        daily = daily.dropna(subset=["date", "permaticker", "marketcap"]).copy()
        if daily.empty:
            return pd.DataFrame(index=date_index)
        pivot = daily.pivot_table(index="date", columns="permaticker", values="marketcap", aggfunc="last").sort_index()
        pivot.columns = [int(column) for column in pivot.columns]
        aligned = pivot.reindex(pivot.index.union(date_index)).sort_index().ffill().reindex(date_index)
        return aligned

    def _get_adjusted_prices(self, table: str, permatickers: Sequence[int | str], start: str, end: str) -> dict[int, pd.DataFrame]:
        wanted = {perma for perma in (_parse_security_permaticker(item) for item in permatickers) if perma is not None}
        sep = self.read_fact_table(
            table,
            columns=["permaticker", "date", "open", "high", "low", "close", "closeadj", "volume"],
            filters=[("permaticker", "in", sorted(wanted))] if wanted else None,
        )
        if sep.empty:
            return {int(perma): pd.DataFrame() for perma in wanted}
        sep = _normalize_columns(sep)
        if "permaticker" not in sep.columns or "date" not in sep.columns:
            return {int(perma): pd.DataFrame() for perma in wanted}
        sep["permaticker"] = pd.to_numeric(sep["permaticker"], errors="coerce").astype("Int64")
        sep["date"] = pd.to_datetime(sep["date"], errors="coerce")
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        rows = sep.loc[sep["permaticker"].isin(wanted) & (sep["date"] >= start_ts) & (sep["date"] <= end_ts)].copy()
        actions = self._delist_dates()
        out: dict[int, pd.DataFrame] = {}
        for perma in wanted:
            frame = rows.loc[rows["permaticker"] == perma].copy()
            delist_date = actions.get(perma)
            if delist_date is not None:
                frame = frame.loc[frame["date"] <= delist_date]
            out[perma] = _price_frame(frame)
        return out

    def get_fundamentals_asof(
        self,
        permaticker: int,
        as_of_date: str | pd.Timestamp,
        fields: Sequence[str],
        *,
        dimensions: Sequence[str] = AS_REPORTED_DIMENSIONS,
    ) -> dict[str, Any] | None:
        as_of = pd.Timestamp(as_of_date)
        sf1 = self.get_fundamentals_history(permaticker, as_of, fields, dimensions=dimensions)
        if sf1.empty:
            return None
        sf1 = sf1.loc[sf1["datekey"] <= as_of].copy()
        if sf1.empty:
            return None
        dim_order = {str(dim).upper(): idx for idx, dim in enumerate(dimensions)}
        sf1["_dimension_order"] = (
            sf1["dimension"].astype(str).str.upper().map(dim_order).fillna(999)
            if "dimension" in sf1.columns
            else 999
        )
        row = sf1.sort_values(["datekey", "_dimension_order"], ascending=[False, True]).iloc[0]
        payload: dict[str, Any] = {
            "permaticker": int(permaticker),
            "datekey": pd.Timestamp(row["datekey"]).date().isoformat(),
            "dimension": str(row.get("dimension") or ""),
        }
        for field in fields:
            normalized = str(field).lower()
            payload[normalized] = row.get(normalized)
        return payload

    def get_fundamentals_history(
        self,
        permaticker: int,
        end_date: str | pd.Timestamp,
        fields: Sequence[str],
        *,
        dimensions: Sequence[str] = AS_REPORTED_DIMENSIONS,
    ) -> pd.DataFrame:
        columns = ["permaticker", "datekey", "dimension", *[str(field).lower() for field in fields]]
        sf1 = self.read_fact_table(
            "SF1",
            columns=list(dict.fromkeys(columns)),
            filters=[("permaticker", "=", int(permaticker))],
        )
        if sf1.empty:
            return pd.DataFrame()
        sf1 = _normalize_columns(sf1)
        if "permaticker" not in sf1.columns or "datekey" not in sf1.columns:
            return pd.DataFrame()
        sf1["permaticker"] = pd.to_numeric(sf1["permaticker"], errors="coerce").astype("Int64")
        sf1["datekey"] = pd.to_datetime(sf1["datekey"], errors="coerce")
        rows = sf1.loc[(sf1["permaticker"] == int(permaticker)) & (sf1["datekey"] <= pd.Timestamp(end_date))].copy()
        if rows.empty:
            return pd.DataFrame()
        if "dimension" in rows.columns:
            rows = rows.loc[rows["dimension"].astype(str).str.upper().isin({str(dim).upper() for dim in dimensions})].copy()
        return rows.sort_values("datekey")

    def universe_asof(self, date: str | pd.Timestamp, *, top_n: int = 500) -> list[int]:
        sp500 = self.read_meta_table("SP500")
        as_of = pd.Timestamp(date).normalize()
        if not sp500.empty and "permaticker" in sp500.columns:
            rows = _normalize_columns(sp500)
            rows["_start"] = _date_series(rows, ["date", "startdate", "fromdate"])
            rows["_end"] = _date_series(rows, ["enddate", "throughdate", "todate"])
            active = rows.loc[
                (rows["_start"].isna() | (rows["_start"] <= as_of))
                & (rows["_end"].isna() | (rows["_end"] >= as_of))
            ]
            values = [int(value) for value in pd.to_numeric(active["permaticker"], errors="coerce").dropna().astype(int).tolist()]
            if values:
                return list(dict.fromkeys(values))
        daily = self.read_fact_table("DAILY")
        if daily.empty or "permaticker" not in daily.columns:
            return []
        daily = _normalize_columns(daily)
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        rows = daily.loc[daily["date"] <= as_of].copy()
        if rows.empty:
            return []
        rows = rows.sort_values(["permaticker", "date"]).groupby("permaticker", as_index=False).tail(1)
        cap_col = _first_existing(rows, ["marketcap", "market_cap", "ev"])
        if cap_col is None:
            return [int(value) for value in pd.to_numeric(rows["permaticker"], errors="coerce").dropna().astype(int).head(top_n).tolist()]
        rows[cap_col] = pd.to_numeric(rows[cap_col], errors="coerce")
        return [int(value) for value in pd.to_numeric(rows.sort_values(cap_col, ascending=False)["permaticker"], errors="coerce").dropna().astype(int).head(top_n).tolist()]

    def _delist_dates(self) -> dict[int, pd.Timestamp]:
        actions = self.read_meta_table("ACTIONS")
        if actions.empty or "permaticker" not in actions.columns:
            return {}
        rows = _normalize_columns(actions)
        event_col = _first_existing(rows, ["action", "event", "type"])
        if event_col is not None:
            mask = rows[event_col].astype(str).str.lower().str.contains(TERMINAL_ACTION_PATTERN, regex=True, na=False)
            rows = rows.loc[mask].copy()
        date_col = _first_existing(rows, ["date", "actiondate"])
        if date_col is None:
            return {}
        rows[date_col] = pd.to_datetime(rows[date_col], errors="coerce")
        out: dict[int, pd.Timestamp] = {}
        for _, row in rows.dropna(subset=[date_col]).iterrows():
            perma = _parse_int(row.get("permaticker"))
            if perma is not None:
                current = out.get(perma)
                value = pd.Timestamp(row[date_col]).normalize()
                out[perma] = min(current, value) if current is not None else value
        return out


def _price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    rows = frame.copy().sort_values("date")
    close_col = _first_existing(rows, ["close", "closeunadj", "price"])
    adj_col = _first_existing(rows, ["closeadj", "adjclose", "adj_close"])
    if close_col is None:
        return pd.DataFrame()
    close = pd.to_numeric(rows[close_col], errors="coerce")
    adjusted = pd.to_numeric(rows[adj_col], errors="coerce") if adj_col else close
    factor = (adjusted / close.replace(0, pd.NA)).fillna(1.0)
    data = pd.DataFrame(index=pd.to_datetime(rows["date"], errors="coerce"))
    factor_values = factor.to_numpy()
    for source, target in (("open", "open"), ("high", "high"), ("low", "low")):
        if source in rows.columns:
            data[target] = pd.to_numeric(rows[source], errors="coerce").to_numpy() * factor_values
        else:
            data[target] = adjusted.to_numpy()
    data["price"] = adjusted.to_numpy()
    if "volume" in rows.columns:
        data["volume"] = pd.to_numeric(rows["volume"], errors="coerce").fillna(0).to_numpy()
    else:
        data["volume"] = 0.0
    if "permaticker" in rows.columns:
        data["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce").astype("Int64").to_numpy()
    return data.dropna(subset=["price", "open"]).sort_index()


def _read_parquet(
    path: Path,
    *,
    columns: Sequence[str] | None = None,
    filters: list[tuple[str, str, Any]] | None = None,
) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=list(columns) if columns else None, filters=filters)
    except Exception:
        try:
            return pd.read_parquet(path, columns=list(columns) if columns else None)
        except Exception:
            return pd.read_parquet(path)


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.columns = [str(column).strip().lower().replace(" ", "_") for column in out.columns]
    return out


def _table_name(table: str) -> str:
    return str(table or "").strip().upper()


def _normalize_ticker(value: str) -> str:
    return str(value or "").strip().upper()


def _parse_int(value: Any) -> int | None:
    try:
        if value is None or str(value) == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _parse_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(str(value).replace(",", ""))
        if pd.isna(parsed):
            return None
        return parsed
    except Exception:
        return None


def _parse_security_permaticker(value: Any) -> int | None:
    text = str(value or "").strip().upper()
    if text.startswith("P") and text[1:].replace(".", "", 1).isdigit():
        text = text[1:]
    return _parse_int(text)


def _unknown_terminal_health_branch(last_price: dict[str, Any], health: dict[str, Any]) -> str:
    price = _parse_float(last_price.get("last_price"))
    if price is None or price <= 0:
        return "distressed_zero"
    if bool(health.get("positive_equity")) or bool(health.get("profitable")):
        return "healthy_last_price"
    if bool(last_price.get("price_collapse")):
        return "distressed_zero"
    if bool(health.get("negative_equity")) and bool(health.get("unprofitable")):
        return "distressed_zero"
    return "healthy_last_price"


def _unknown_terminal_branch_reason(last_price: dict[str, Any], health: dict[str, Any]) -> str:
    parts: list[str] = []
    price = _parse_float(last_price.get("last_price"))
    if price is None:
        parts.append("no usable pre-delisting SEP price")
    else:
        parts.append(f"last_price={price:.4f}")
    drawdown = _parse_float(last_price.get("drawdown_pct"))
    if drawdown is not None:
        parts.append(f"drawdown_from_trailing_high={drawdown:.1%}")
    if health:
        if health.get("positive_equity"):
            parts.append("positive_equity")
        if health.get("negative_equity"):
            parts.append("negative_equity")
        if health.get("profitable"):
            parts.append("profitable")
        if health.get("unprofitable"):
            parts.append("unprofitable")
    else:
        parts.append("no PIT fundamentals near delist")
    return "; ".join(parts)


def _matches(value: object, pattern: str) -> bool:
    return bool(re.search(pattern, str(value or ""), flags=re.IGNORECASE))


def _choose_terminal_detail(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    if current is None:
        return candidate
    current_source = str(current.get("source") or "")
    candidate_source = str(candidate.get("source") or "")
    if current_source == "TICKERS" and candidate_source == "ACTIONS":
        return candidate
    if current_source == "ACTIONS" and candidate_source == "TICKERS":
        return current
    current_date = pd.Timestamp(current.get("date")).normalize()
    candidate_date = pd.Timestamp(candidate.get("date")).normalize()
    if candidate_date < current_date:
        return candidate
    return current


def _terminal_category_breakdown(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    breakdown: dict[str, int] = {
        "actions_failure_default_zero": 0,
        "metadata_failure_default_zero": 0,
        "seeded_failure_default_zero": 0,
        "acquisition_last_price": 0,
        "acquisition_missing_price_zero": 0,
        "unknown_healthy_last_price": 0,
        "unknown_distressed_zero": 0,
        "unknown_missing_price_zero": 0,
    }
    for row in rows:
        source = str(row.get("source") or "")
        if source in breakdown:
            breakdown[source] += 1
        else:
            breakdown[source] = breakdown.get(source, 0) + 1
    return {key: int(value) for key, value in breakdown.items() if value}


def _first_existing(frame: pd.DataFrame, names: Sequence[str]) -> str | None:
    columns = set(frame.columns)
    for name in names:
        if name in columns:
            return name
    return None


def _date_series(frame: pd.DataFrame, names: Sequence[str]) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return pd.to_datetime(frame[name], errors="coerce")
    return pd.Series(pd.NaT, index=frame.index)


def _monthly_trading_rebalance_dates(date_index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    if date_index.empty:
        return []
    calendar_dates = [start.normalize()]
    calendar_dates.extend(pd.Timestamp(date).normalize() for date in pd.date_range(start.normalize(), end.normalize(), freq="MS") if pd.Timestamp(date).normalize() > start.normalize())
    out: list[pd.Timestamp] = []
    for calendar_date in sorted(set(calendar_dates)):
        eligible = date_index[date_index >= calendar_date]
        if len(eligible):
            trade_date = pd.Timestamp(eligible[0]).normalize()
            if not out or out[-1] != trade_date:
                out.append(trade_date)
    return out


def _synth_weights_for_date(
    members: Sequence[int],
    date: pd.Timestamp,
    *,
    price_panel: pd.DataFrame,
    cap_panel: pd.DataFrame,
) -> pd.Series:
    if not members or date not in price_panel.index:
        return pd.Series(dtype="float64")
    available = price_panel.loc[date, [member for member in members if member in price_panel.columns]].dropna()
    if available.empty:
        return pd.Series(dtype="float64")
    eligible = [int(member) for member in available.index]
    caps = pd.Series(dtype="float64")
    if not cap_panel.empty and date in cap_panel.index:
        caps = cap_panel.loc[date, [member for member in eligible if member in cap_panel.columns]]
        caps = pd.to_numeric(caps, errors="coerce").dropna()
        caps = caps.loc[caps > 0]
    if caps.empty:
        weights = pd.Series(1.0 / len(eligible), index=eligible, dtype="float64")
    else:
        weights = caps.astype(float) / float(caps.sum())
    weights.index = [int(index) for index in weights.index]
    return weights.sort_index()
