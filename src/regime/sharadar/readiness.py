from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

import pandas as pd

from .store import DEFAULT_SHARADAR_DIR, SharadarStore

READINESS_PRICE_ONLY = "price_only_proxy"
READINESS_PARTIAL_PIT = "partial_pit"
READINESS_SURVIVORSHIP_FREE = "survivorship_free"


@dataclass(frozen=True)
class DataReadinessResult:
    data_readiness: str
    price_coverage: bool
    terminal_coverage: bool
    pit_fundamental_coverage: bool
    price_coverage_ratio: float
    terminal_coverage_ratio: float
    pit_fundamental_coverage_ratio: float
    universe_count: int
    resolved_count: int
    missing_price: tuple[str, ...]
    missing_terminal: tuple[str, ...]
    missing_pit: tuple[str, ...]
    fundamental_exceptions: tuple[str, ...]
    data_snapshot_hash: str | None
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def classify_readiness(
    store: SharadarStore | str | Path = DEFAULT_SHARADAR_DIR,
    universe: Sequence[str | int] | None = None,
    date_range: tuple[str, str] | None = None,
) -> DataReadinessResult:
    sharadar_store = store if isinstance(store, SharadarStore) else SharadarStore(store)
    if not sharadar_store.exists():
        return DataReadinessResult(
            data_readiness=READINESS_PRICE_ONLY,
            price_coverage=False,
            terminal_coverage=False,
            pit_fundamental_coverage=False,
            price_coverage_ratio=0.0,
            terminal_coverage_ratio=0.0,
            pit_fundamental_coverage_ratio=0.0,
            universe_count=len(universe or []),
            resolved_count=0,
            missing_price=tuple(str(item).upper() for item in (universe or [])),
            missing_terminal=tuple(str(item).upper() for item in (universe or [])),
            missing_pit=tuple(str(item).upper() for item in (universe or [])),
            fundamental_exceptions=(),
            data_snapshot_hash=None,
            reasons=("no_local_sharadar_snapshot",),
        )

    start, end = date_range or ("1900-01-01", pd.Timestamp.today().date().isoformat())
    start_ts = pd.Timestamp(start)
    raw_identifiers = [item for item in (universe or []) if str(item).strip()]
    if not raw_identifiers:
        raw_identifiers = list(sharadar_store.universe_asof(start_ts))

    resolved: dict[str, int] = {}
    unresolved: list[str] = []
    for raw_identifier in raw_identifiers:
        identifier = str(raw_identifier).strip().upper()
        direct_permaticker = _parse_security_permaticker(raw_identifier)
        resolution = None if direct_permaticker is not None else sharadar_store.resolve_ticker(identifier, as_of_date=start_ts)
        permaticker = direct_permaticker if direct_permaticker is not None else (resolution.permaticker if resolution is not None else None)
        if permaticker is None:
            unresolved.append(identifier)
            continue
        resolved[identifier] = int(permaticker)

    missing_price: list[str] = list(unresolved)
    missing_terminal: list[str] = []
    missing_pit: list[str] = list(unresolved)
    unique_permatickers = sorted(set(resolved.values()))
    price_frames = sharadar_store.get_prices(unique_permatickers, start=start, end=end) if unique_permatickers else {}
    price_ok_by_perma = {
        int(permaticker)
        for permaticker, frame in price_frames.items()
        if frame is not None and not frame.empty
    }
    pit_ok_by_perma = _fundamental_coverage_permatickers(sharadar_store, unique_permatickers, end)
    delisted_permas = sharadar_store.delisted_permatickers(unique_permatickers, end=end)
    terminal_events = sharadar_store.terminal_value_events(unique_permatickers, end=end)
    price_ok_count = 0
    terminal_ok_count = 0
    pit_ok_count = 0
    for identifier, permaticker in resolved.items():
        if permaticker not in price_ok_by_perma:
            missing_price.append(identifier)
        else:
            price_ok_count += 1
        if permaticker not in delisted_permas or permaticker in terminal_events:
            terminal_ok_count += 1
        else:
            missing_terminal.append(identifier)
        if permaticker not in pit_ok_by_perma:
            missing_pit.append(identifier)
        else:
            pit_ok_count += 1

    price_coverage = bool(resolved) and price_ok_count == len(resolved) and not unresolved
    terminal_coverage = bool(resolved) and terminal_ok_count == len(resolved) and not unresolved
    pit_coverage = bool(resolved) and pit_ok_count == len(resolved) and not unresolved
    denominator = len(resolved) + len(unresolved)
    price_coverage_ratio = price_ok_count / denominator if denominator else 0.0
    terminal_coverage_ratio = terminal_ok_count / denominator if denominator else 0.0
    pit_coverage_ratio = pit_ok_count / denominator if denominator else 0.0
    fundamental_exceptions = sorted(set(missing_pit) - set(unresolved))
    reasons: list[str] = []
    if missing_price:
        reasons.append("missing_adjusted_price_history")
    if missing_terminal:
        reasons.append("missing_terminal_value_handling")
    if fundamental_exceptions:
        reasons.append("missing_point_in_time_fundamentals_documented_exception")

    if price_coverage and terminal_coverage and resolved:
        readiness = READINESS_SURVIVORSHIP_FREE
    elif price_ok_count > 0:
        readiness = READINESS_PARTIAL_PIT
    else:
        readiness = READINESS_PRICE_ONLY
    if not reasons:
        reasons.append("local_sharadar_snapshot_ready")

    return DataReadinessResult(
        data_readiness=readiness,
        price_coverage=price_coverage,
        terminal_coverage=terminal_coverage,
        pit_fundamental_coverage=pit_coverage,
        price_coverage_ratio=price_coverage_ratio,
        terminal_coverage_ratio=terminal_coverage_ratio,
        pit_fundamental_coverage_ratio=pit_coverage_ratio,
        universe_count=len(raw_identifiers),
        resolved_count=len(resolved),
        missing_price=tuple(sorted(missing_price)),
        missing_terminal=tuple(sorted(missing_terminal)),
        missing_pit=tuple(sorted(missing_pit)),
        fundamental_exceptions=tuple(fundamental_exceptions),
        data_snapshot_hash=sharadar_store.data_snapshot_hash,
        reasons=tuple(reasons),
    )


def certification_gate_status(
    data_readiness: str | DataReadinessResult,
    *,
    after_tax: bool = True,
    out_of_sample: bool = True,
    killed: bool = False,
) -> str:
    readiness = data_readiness.data_readiness if isinstance(data_readiness, DataReadinessResult) else str(data_readiness)
    if killed:
        return "killed"
    if readiness == READINESS_SURVIVORSHIP_FREE and after_tax and out_of_sample:
        return "certifiable"
    return "research_only_not_certifiable"


def _parse_int(value: object) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except Exception:
        return None


def _parse_security_permaticker(value: object) -> int | None:
    text = str(value or "").strip().upper()
    if text.startswith("P") and text[1:].replace(".", "", 1).isdigit():
        text = text[1:]
    return _parse_int(text)


def _fundamental_coverage_permatickers(
    store: SharadarStore,
    permatickers: Sequence[int],
    end: str,
) -> set[int]:
    wanted = sorted({int(permaticker) for permaticker in permatickers})
    if not wanted:
        return set()
    aliases = _issuer_fundamental_aliases(store, wanted)
    lookup_permatickers = sorted(set(wanted) | set(aliases.values()))
    sf1 = store.read_fact_table(
        "SF1",
        columns=["permaticker", "datekey", "dimension", "netinc", "assets", "revenue"],
        filters=[("permaticker", "in", lookup_permatickers), ("datekey", "<=", str(end))],
    )
    if sf1.empty or "permaticker" not in sf1.columns or "datekey" not in sf1.columns:
        return set()
    rows = sf1.copy()
    rows.columns = [str(column).strip().lower().replace(" ", "_") for column in rows.columns]
    rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
    rows["datekey"] = pd.to_datetime(rows["datekey"], errors="coerce")
    rows = rows.loc[rows["permaticker"].isin(lookup_permatickers) & (rows["datekey"] <= pd.Timestamp(end))].copy()
    if "dimension" in rows.columns:
        rows = rows.loc[rows["dimension"].astype(str).str.upper().isin({str(dim).upper() for dim in ("ARQ", "ART", "ARY")})].copy()
    required = [column for column in ("netinc", "assets", "revenue") if column in rows.columns]
    if required:
        rows = rows.dropna(subset=required, how="all")
    covered_lookup = {int(value) for value in pd.to_numeric(rows["permaticker"], errors="coerce").dropna().astype(int).unique().tolist()}
    return {perma for perma in wanted if perma in covered_lookup or aliases.get(perma) in covered_lookup}


def _issuer_fundamental_aliases(store: SharadarStore, permatickers: Sequence[int]) -> dict[int, int]:
    tickers = store.read_meta_table("TICKERS")
    if tickers.empty or "permaticker" not in tickers.columns or "name" not in tickers.columns:
        return {}
    rows = tickers.copy()
    rows.columns = [str(column).strip().lower().replace(" ", "_") for column in rows.columns]
    rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
    if "table" not in rows.columns:
        rows["table"] = ""
    if "category" not in rows.columns:
        rows["category"] = ""
    aliases: dict[int, int] = {}
    for perma in sorted({int(value) for value in permatickers}):
        issuer_rows = rows.loc[rows["permaticker"] == perma].copy()
        if issuer_rows.empty:
            continue
        names = [str(value).strip().upper() for value in issuer_rows["name"].dropna().tolist() if str(value).strip()]
        if not names:
            continue
        same_issuer = rows.loc[rows["name"].astype(str).str.strip().str.upper().isin(set(names))].copy()
        candidates = same_issuer.loc[
            (same_issuer["permaticker"] != perma)
            & (same_issuer["table"].astype(str).str.upper() == "SF1")
            & same_issuer["category"].astype(str).str.upper().str.contains("COMMON STOCK", na=False)
        ].copy()
        if candidates.empty:
            continue
        candidates["_primary"] = candidates["category"].astype(str).str.upper().str.contains("PRIMARY CLASS", na=False)
        candidates = candidates.sort_values(["_primary", "permaticker"], ascending=[False, True])
        aliases[perma] = int(candidates.iloc[0]["permaticker"])
    return aliases
