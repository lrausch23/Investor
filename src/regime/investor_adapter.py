from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .config import EXCLUDED_TICKER_PATTERNS, HMM_ELIGIBLE_ASSET_CLASSES, ticker_candidates
from .market_data_client import download_daily_bars, get_ticker_info
from .persistence import get_cached_sector, save_sector_cache

DEFAULT_INVESTOR_DB_PATHS = (
    Path(__file__).resolve().parents[2] / "data" / "investor.db",
)
DEFAULT_INVESTOR_DB_PATH = DEFAULT_INVESTOR_DB_PATHS[0]


@dataclass(frozen=True)
class TaxLotInfo:
    lot_id: int
    acquisition_date: str
    qty: float
    basis_total: float
    days_held: int
    term: str
    unrealized_gain: float
    days_to_ltcg: int


@dataclass(frozen=True)
class PortfolioPosition:
    ticker: str
    account_name: str
    account_type: str
    taxpayer_type: str
    qty: float
    market_value: float
    current_price: float
    cost_basis: float
    unrealized_gain: float
    asset_class: str
    lots: list[TaxLotInfo]


def get_investor_db_path() -> str | None:
    configured = os.getenv("INVESTOR_DB_PATH")
    if configured and Path(configured).exists():
        return configured
    if DEFAULT_INVESTOR_DB_PATH != DEFAULT_INVESTOR_DB_PATHS[0]:
        return str(DEFAULT_INVESTOR_DB_PATH) if DEFAULT_INVESTOR_DB_PATH.exists() else None
    if DEFAULT_INVESTOR_DB_PATH.exists():
        return str(DEFAULT_INVESTOR_DB_PATH)
    for candidate in DEFAULT_INVESTOR_DB_PATHS:
        if candidate.exists():
            return str(candidate)
    return None


def _connect(db_path: str) -> sqlite3.Connection:
    uri = f"file:{Path(db_path).resolve().as_posix()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_last_price(metadata_json: str | None) -> float:
    if not metadata_json:
        return 0.0
    try:
        parsed = json.loads(metadata_json)
    except json.JSONDecodeError:
        return 0.0
    return float(parsed.get("last_price") or 0.0)


def _parse_provider_symbol(metadata_json: str | None, fallback_ticker: str) -> str:
    if not metadata_json:
        return fallback_ticker
    try:
        parsed = json.loads(metadata_json)
    except json.JSONDecodeError:
        return fallback_ticker
    return str(parsed.get("provider_symbol") or fallback_ticker)


def _fetch_live_market_price(ticker: str, metadata_json: str | None = None) -> float:
    provider_symbol = _parse_provider_symbol(metadata_json, ticker)
    for candidate in ticker_candidates(provider_symbol):
        try:
            history = download_daily_bars(candidate, period="5d", auto_adjust=True)
        except Exception:
            continue
        if history.empty:
            continue
        if getattr(history.columns, "nlevels", 1) > 1:
            if candidate in history.columns.get_level_values(-1):
                history = history.xs(candidate, axis=1, level=-1)
            elif candidate in history.columns.get_level_values(0):
                history = history.xs(candidate, axis=1, level=0)
        close_col = "Close" if "Close" in history.columns else history.columns[0]
        series = history[close_col].dropna()
        if not series.empty:
            return float(series.iloc[-1])
    return 0.0


def _fetch_live_market_prices(metadata_by_ticker: dict[str, str | None]) -> dict[str, float]:
    candidate_to_ticker: dict[str, str] = {}
    for ticker, metadata_json in metadata_by_ticker.items():
        provider_symbol = _parse_provider_symbol(metadata_json, ticker)
        for candidate in ticker_candidates(provider_symbol):
            candidate_to_ticker.setdefault(candidate, ticker.upper())

    if not candidate_to_ticker:
        return {}

    try:
        history = download_daily_bars(list(candidate_to_ticker), period="5d", auto_adjust=True, group_by="ticker")
    except Exception:
        return {}
    if history.empty:
        return {}

    live_prices: dict[str, float] = {}
    if getattr(history.columns, "nlevels", 1) == 1:
        close_col = "Close" if "Close" in history.columns else history.columns[0]
        series = history[close_col].dropna()
        if not series.empty and len(candidate_to_ticker) == 1:
            only_ticker = next(iter(candidate_to_ticker.values()))
            live_prices[only_ticker] = float(series.iloc[-1])
        return live_prices

    for candidate, canonical_ticker in candidate_to_ticker.items():
        if canonical_ticker in live_prices:
            continue
        if candidate not in history.columns.get_level_values(0):
            continue
        candidate_frame = history[candidate]
        close_col = "Close" if "Close" in candidate_frame.columns else candidate_frame.columns[0]
        series = candidate_frame[close_col].dropna()
        if not series.empty:
            live_prices[canonical_ticker] = float(series.iloc[-1])
    return live_prices


def _resolve_current_price(conn: sqlite3.Connection, ticker: str, metadata_json: str | None = None) -> float:
    try:
        row = conn.execute(
            """
            SELECT COALESCE(adj_close, close) AS price
            FROM price_daily
            WHERE ticker = ?
              AND COALESCE(adj_close, close) IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
            """,
            (ticker.upper(),),
        ).fetchone()
        if row and row["price"] is not None:
            return float(row["price"])
    except sqlite3.Error:
        pass
    metadata_price = _parse_last_price(metadata_json)
    if metadata_price > 0:
        return metadata_price
    return _fetch_live_market_price(ticker, metadata_json)


def get_latest_prices(db_path: str | None, tickers: list[str]) -> dict[str, float]:
    if not db_path or not tickers:
        return {}
    unique_tickers = sorted({ticker.upper() for ticker in tickers if ticker})
    placeholders = ", ".join("?" for _ in unique_tickers)
    prices: dict[str, float] = {}
    try:
        with _connect(db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT pd.ticker, COALESCE(pd.adj_close, pd.close) AS price
                FROM price_daily pd
                JOIN (
                    SELECT ticker, MAX(date) AS max_date
                    FROM price_daily
                    WHERE ticker IN ({placeholders})
                      AND COALESCE(adj_close, close) IS NOT NULL
                    GROUP BY ticker
                ) latest
                  ON latest.ticker = pd.ticker
                 AND latest.max_date = pd.date
                """,
                unique_tickers,
            ).fetchall()
    except sqlite3.Error:
        return {}
    for row in rows:
        if row["ticker"] and row["price"] is not None:
            prices[str(row["ticker"]).upper()] = float(row["price"])
    missing = [ticker for ticker in unique_tickers if ticker not in prices]
    if not missing:
        return prices
    metadata_by_ticker: dict[str, str | None] = {}
    try:
        with _connect(db_path) as conn:
            meta_rows = conn.execute(
                f"""
                SELECT ticker, metadata_json
                FROM securities
                WHERE ticker IN ({placeholders})
                """,
                unique_tickers,
            ).fetchall()
            metadata_by_ticker = {str(row["ticker"]).upper(): row["metadata_json"] for row in meta_rows if row["ticker"]}
    except sqlite3.Error:
        metadata_by_ticker = {}
    prices.update(_fetch_live_market_prices({ticker: metadata_by_ticker.get(ticker) for ticker in missing}))
    return prices


def get_sector_map(db_path: str | None, tickers: list[str]) -> dict[str, str]:
    if not tickers:
        return {}
    unique_tickers = sorted({str(ticker).upper() for ticker in tickers if str(ticker or "").strip()})
    sectors: dict[str, str] = {}
    missing = list(unique_tickers)
    if db_path:
        placeholders = ", ".join("?" for _ in unique_tickers)
        try:
            with _connect(db_path) as conn:
                rows = conn.execute(
                    f"""
                    SELECT ticker, COALESCE(NULLIF(sector, ''), NULLIF(industry, '')) AS sector
                    FROM securities
                    WHERE ticker IN ({placeholders})
                    """,
                    unique_tickers,
                ).fetchall()
            for row in rows:
                ticker = str(row["ticker"] or "").upper()
                sector = str(row["sector"] or "").strip()
                if ticker and sector:
                    sectors[ticker] = sector
        except sqlite3.Error:
            pass
        missing = [ticker for ticker in unique_tickers if ticker not in sectors or sectors[ticker].lower() == "unknown"]

    for ticker in list(missing):
        cached = get_cached_sector(ticker)
        if cached:
            sectors[ticker] = cached

    unresolved = [ticker for ticker in unique_tickers if ticker not in sectors]
    for ticker in unresolved:
        info = get_ticker_info(ticker)
        sector = str(info.get("sector") or info.get("industry") or "").strip()
        if sector:
            sectors[ticker] = sector
            save_sector_cache(ticker, sector)
        else:
            sectors.setdefault(ticker, "Unknown")
    return sectors


def _is_hmm_eligible_ticker(ticker: str, asset_class: str) -> bool:
    symbol = ticker.upper()
    normalized_asset_class = (asset_class or "UNKNOWN").upper()
    if normalized_asset_class not in HMM_ELIGIBLE_ASSET_CLASSES:
        return False
    if symbol in EXCLUDED_TICKER_PATTERNS:
        return False
    if symbol.startswith("^") or "=" in symbol:
        return False
    return True


def get_portfolio_tickers(db_path: str | None) -> list[str]:
    if not db_path:
        return []
    try:
        with _connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT ticker FROM (
                    SELECT DISTINCT ticker
                    FROM position_lots
                    WHERE CAST(qty AS REAL) > 0
                    UNION
                    SELECT DISTINCT s.ticker
                    FROM tax_lots tl
                    JOIN securities s ON s.id = tl.security_id
                    WHERE tl.source = 'RECONSTRUCTED' AND CAST(tl.quantity_open AS REAL) > 0
                )
                ORDER BY ticker
                """
            ).fetchall()
    except sqlite3.Error:
        return []
    return sorted({str(row["ticker"]).upper() for row in rows if row["ticker"]})


def get_current_holding_tickers_grouped(db_path: str | None) -> dict[str, list[str]]:
    if not db_path:
        return {}
    try:
        with _connect(db_path) as conn:
            connection_rows = conn.execute(
                """
                SELECT ec.id
                FROM external_connections ec
                WHERE COALESCE(ec.status, 'ACTIVE') = 'ACTIVE'
                """
            ).fetchall()
            connection_ids = [int(row["id"]) for row in connection_rows if row["id"] is not None]
            if not connection_ids:
                return {}

            placeholders = ", ".join("?" for _ in connection_ids)
            snapshot_rows = conn.execute(
                f"""
                SELECT ehs.connection_id, ehs.payload_json
                FROM external_holding_snapshots ehs
                JOIN (
                    SELECT connection_id, MAX(as_of) AS max_as_of
                    FROM external_holding_snapshots
                    WHERE connection_id IN ({placeholders})
                    GROUP BY connection_id
                ) latest
                  ON latest.connection_id = ehs.connection_id
                 AND latest.max_as_of = ehs.as_of
                WHERE ehs.connection_id IN ({placeholders})
                ORDER BY ehs.connection_id, ehs.id DESC
                """,
                [*connection_ids, *connection_ids],
            ).fetchall()
            mapping_rows = conn.execute(
                f"""
                SELECT
                    eam.connection_id,
                    eam.provider_account_id,
                    COALESCE(te.type, 'PERSONAL') AS taxpayer_type
                FROM external_account_maps eam
                JOIN accounts a ON a.id = eam.account_id
                JOIN taxpayer_entities te ON te.id = a.taxpayer_entity_id
                WHERE eam.connection_id IN ({placeholders})
                """,
                connection_ids,
            ).fetchall()
    except sqlite3.Error:
        return {}

    taxpayer_by_map = {
        (int(row["connection_id"]), str(row["provider_account_id"])): str(row["taxpayer_type"] or "PERSONAL").upper()
        for row in mapping_rows
        if row["connection_id"] is not None and row["provider_account_id"]
    }
    grouped: dict[str, set[str]] = {}
    seen_connections: set[int] = set()
    for row in snapshot_rows:
        connection_id = int(row["connection_id"])
        if connection_id in seen_connections:
            continue
        seen_connections.add(connection_id)
        payload = row["payload_json"] or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        items = payload.get("items") or []
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict) or item.get("is_total"):
                continue
            ticker = str(item.get("symbol") or item.get("ticker") or "").strip().upper()
            provider_account_id = str(item.get("provider_account_id") or "").strip()
            if not ticker or ticker.startswith("CASH:"):
                continue
            taxpayer_type = taxpayer_by_map.get((connection_id, provider_account_id), "PERSONAL")
            label = "Trust" if taxpayer_type == "TRUST" else "Personal"
            grouped.setdefault(label, set()).add(ticker)
    return {label: sorted(values) for label, values in grouped.items() if values}


def get_current_holding_tickers(db_path: str | None) -> list[str]:
    grouped = get_current_holding_tickers_grouped(db_path)
    tickers = sorted({ticker for values in grouped.values() for ticker in values})
    return tickers


def get_portfolio_tickers_filtered(db_path: str | None) -> list[str]:
    all_tickers = get_portfolio_tickers(db_path)
    if not db_path or not all_tickers:
        return []
    placeholders = ", ".join("?" for _ in all_tickers)
    asset_classes: dict[str, str] = {}
    try:
        with _connect(db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT ticker, COALESCE(asset_class, 'UNKNOWN') AS asset_class
                FROM securities
                WHERE ticker IN ({placeholders})
                """,
                all_tickers,
            ).fetchall()
            asset_classes = {str(row["ticker"]).upper(): str(row["asset_class"] or "UNKNOWN") for row in rows if row["ticker"]}
    except sqlite3.Error:
        asset_classes = {}
    filtered = [
        ticker
        for ticker in all_tickers
        if _is_hmm_eligible_ticker(ticker, asset_classes.get(ticker, "UNKNOWN"))
    ]
    return sorted(filtered)


def get_portfolio_positions(
    db_path: str | None,
    tickers: list[str] | None = None,
    account_id: int | None = None,
) -> list[PortfolioPosition]:
    if not db_path:
        return []

    today = date.today()
    selected_tickers = sorted({ticker.upper() for ticker in (tickers or []) if ticker})
    where_clauses_tax = ["tl.source = 'RECONSTRUCTED'", "CAST(tl.quantity_open AS REAL) > 0"]
    where_clauses_pos = ["CAST(pl.qty AS REAL) > 0"]
    distinct_pos_where = ["ticker IS NOT NULL"]
    distinct_tax_where = ["s.ticker IS NOT NULL"]
    params: list[Any] = []
    if selected_tickers:
        placeholders = ", ".join("?" for _ in selected_tickers)
        distinct_pos_where.append(f"ticker IN ({placeholders})")
        distinct_tax_where.append(f"s.ticker IN ({placeholders})")
        where_clauses_tax.append(f"s.ticker IN ({placeholders})")
        where_clauses_pos.append(f"pl.ticker IN ({placeholders})")
        params.extend(selected_tickers)
    if account_id is not None:
        distinct_pos_where.append("account_id = ?")
        distinct_tax_where.append("tl.account_id = ?")
        where_clauses_tax.append("tl.account_id = ?")
        where_clauses_pos.append("pl.account_id = ?")
        params.append(int(account_id))
    distinct_pos_sql = " AND ".join(distinct_pos_where)
    distinct_tax_sql = " AND ".join(distinct_tax_where)
    tax_sql = " AND ".join(where_clauses_tax)
    pos_sql = " AND ".join(where_clauses_pos)
    try:
        with _connect(db_path) as conn:
            current_prices = get_latest_prices(
                db_path,
                [
                    *[
                        str(row["ticker"]).upper()
                        for row in conn.execute(
                            f"SELECT DISTINCT ticker FROM position_lots WHERE {distinct_pos_sql}",
                            params,
                        ).fetchall()
                    ],
                    *[
                        str(row["ticker"]).upper()
                        for row in conn.execute(
                            f"""
                            SELECT DISTINCT s.ticker AS ticker
                            FROM tax_lots tl
                            JOIN securities s ON s.id = tl.security_id
                            WHERE {distinct_tax_sql}
                            """,
                            params,
                        ).fetchall()
                    ],
                ],
            )
            tax_rows = conn.execute(
                f"""
                SELECT
                    tl.id AS lot_id,
                    a.id AS account_id,
                    a.name AS account_name,
                    a.account_type AS account_type,
                    te.type AS taxpayer_type,
                    s.ticker AS ticker,
                    s.asset_class AS asset_class,
                    s.metadata_json AS metadata_json,
                    tl.acquired_date AS acquisition_date,
                    CAST(tl.quantity_open AS REAL) AS qty,
                    CAST(COALESCE(tl.basis_open, 0) AS REAL) AS basis_total
                FROM tax_lots tl
                JOIN accounts a ON a.id = tl.account_id
                JOIN taxpayer_entities te ON te.id = a.taxpayer_entity_id
                JOIN securities s ON s.id = tl.security_id
                WHERE {tax_sql}
                ORDER BY a.id, s.ticker, tl.acquired_date
                """,
                params,
            ).fetchall()
            pos_rows = conn.execute(
                f"""
                SELECT
                    pl.id AS lot_id,
                    a.id AS account_id,
                    a.name AS account_name,
                    a.account_type AS account_type,
                    te.type AS taxpayer_type,
                    pl.ticker AS ticker,
                    COALESCE(s.asset_class, 'UNKNOWN') AS asset_class,
                    s.metadata_json AS metadata_json,
                    pl.acquisition_date AS acquisition_date,
                    CAST(pl.qty AS REAL) AS qty,
                    CAST(COALESCE(pl.adjusted_basis_total, pl.basis_total, 0) AS REAL) AS basis_total
                FROM position_lots pl
                JOIN accounts a ON a.id = pl.account_id
                JOIN taxpayer_entities te ON te.id = a.taxpayer_entity_id
                LEFT JOIN securities s ON s.ticker = pl.ticker
                WHERE {pos_sql}
                ORDER BY a.id, pl.ticker, pl.acquisition_date
                """,
                params,
            ).fetchall()
    except sqlite3.Error:
        return []

    tax_by_key: dict[tuple[int, str], list[sqlite3.Row]] = {}
    for row in tax_rows:
        key = (int(row["account_id"]), str(row["ticker"]).upper())
        tax_by_key.setdefault(key, []).append(row)

    pos_by_key: dict[tuple[int, str], list[sqlite3.Row]] = {}
    for row in pos_rows:
        key = (int(row["account_id"]), str(row["ticker"]).upper())
        pos_by_key.setdefault(key, []).append(row)

    all_keys = sorted(set(pos_by_key) | set(tax_by_key))
    positions: list[PortfolioPosition] = []
    for account_id, ticker in all_keys:
        rows = tax_by_key.get((account_id, ticker)) or pos_by_key.get((account_id, ticker), [])
        if not rows:
            continue
        account_name = str(rows[0]["account_name"])
        account_type = str(rows[0]["account_type"] or "OTHER").upper()
        taxpayer_type = str(rows[0]["taxpayer_type"] or "PERSONAL").upper()
        asset_class = str(rows[0]["asset_class"] or "UNKNOWN")
        current_price = current_prices.get(ticker, _parse_last_price(rows[0]["metadata_json"]))
        lots: list[TaxLotInfo] = []
        total_qty = 0.0
        total_basis = 0.0
        for row in rows:
            qty = float(row["qty"] or 0.0)
            basis_total = float(row["basis_total"] or 0.0)
            acquisition_date = str(row["acquisition_date"])
            acquired = date.fromisoformat(acquisition_date)
            days_held = max(0, (today - acquired).days)
            term = "LT" if days_held >= 365 else "ST"
            unrealized_gain = (current_price - (basis_total / qty if qty else 0.0)) * qty
            lots.append(
                TaxLotInfo(
                    lot_id=int(row["lot_id"]),
                    acquisition_date=acquisition_date,
                    qty=qty,
                    basis_total=basis_total,
                    days_held=days_held,
                    term=term,
                    unrealized_gain=unrealized_gain,
                    days_to_ltcg=max(0, 365 - days_held),
                )
            )
            total_qty += qty
            total_basis += basis_total
        market_value = total_qty * current_price
        positions.append(
            PortfolioPosition(
                ticker=ticker,
                account_name=account_name,
                account_type=account_type,
                taxpayer_type=taxpayer_type,
                qty=total_qty,
                market_value=market_value,
                current_price=current_price,
                cost_basis=total_basis,
                unrealized_gain=market_value - total_basis,
                asset_class=asset_class,
                lots=lots,
            )
        )
    return positions


def positions_by_ticker_and_account(positions: list[PortfolioPosition]) -> dict[str, list[PortfolioPosition]]:
    grouped: dict[str, list[PortfolioPosition]] = {}
    for position in positions:
        grouped.setdefault(position.ticker.upper(), []).append(position)
    for entries in grouped.values():
        entries.sort(key=lambda position: (position.account_name, position.account_type))
    return grouped


def positions_by_ticker(positions: list[PortfolioPosition]) -> dict[str, PortfolioPosition]:
    grouped = positions_by_ticker_and_account(positions)

    collapsed: dict[str, PortfolioPosition] = {}
    for ticker, entries in grouped.items():
        if len(entries) == 1:
            collapsed[ticker] = entries[0]
            continue
        account_names = ", ".join(sorted({entry.account_name for entry in entries}))
        account_type = "TAXABLE" if any(entry.account_type == "TAXABLE" for entry in entries) else entries[0].account_type
        taxpayer_types = sorted({entry.taxpayer_type for entry in entries})
        taxpayer_type = taxpayer_types[0] if len(taxpayer_types) == 1 else "PERSONAL"
        qty = sum(entry.qty for entry in entries)
        market_value = sum(entry.market_value for entry in entries)
        cost_basis = sum(entry.cost_basis for entry in entries)
        current_price = (market_value / qty) if qty else entries[0].current_price
        asset_class = entries[0].asset_class
        lots = [lot for entry in entries for lot in entry.lots]
        collapsed[ticker] = PortfolioPosition(
            ticker=ticker,
            account_name=account_names,
            account_type=account_type,
            taxpayer_type=taxpayer_type,
            qty=qty,
            market_value=market_value,
            current_price=current_price,
            cost_basis=cost_basis,
            unrealized_gain=market_value - cost_basis,
            asset_class=asset_class,
            lots=lots,
        )
    return collapsed


def get_wash_sale_risk(db_path: str | None, ticker: str) -> str:
    if not db_path:
        return "NONE"
    today = date.today().isoformat()
    try:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT substitute_group_id FROM securities WHERE ticker = ?",
                (ticker.upper(),),
            ).fetchone()
            substitute_group_id = row["substitute_group_id"] if row else None
            definite = conn.execute(
                """
                SELECT 1
                FROM transactions
                WHERE type = 'BUY'
                  AND ticker = ?
                  AND date BETWEEN date(?, '-30 day') AND date(?, '+30 day')
                LIMIT 1
                """,
                (ticker.upper(), today, today),
            ).fetchone()
            if definite:
                return "DEFINITE"
            if substitute_group_id is not None:
                possible = conn.execute(
                    """
                    SELECT 1
                    FROM transactions t
                    JOIN securities s ON s.ticker = t.ticker
                    WHERE t.type = 'BUY'
                      AND s.substitute_group_id = ?
                      AND date BETWEEN date(?, '-30 day') AND date(?, '+30 day')
                    LIMIT 1
                    """,
                    (substitute_group_id, today, today),
                ).fetchone()
                if possible:
                    return "POSSIBLE"
    except sqlite3.Error:
        return "NONE"
    return "NONE"


def get_tax_assumptions(db_path: str | None) -> dict[str, float]:
    defaults = {"ordinary_rate": 0.37, "ltcg_rate": 0.20, "state_rate": 0.05, "niit_rate": 0.038}
    if not db_path:
        return defaults
    try:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT json_definition FROM tax_assumptions WHERE name = 'Default' LIMIT 1"
            ).fetchone()
    except sqlite3.Error:
        return defaults
    if not row or not row["json_definition"]:
        return defaults
    try:
        parsed = json.loads(row["json_definition"])
    except json.JSONDecodeError:
        return defaults
    return {
        "ordinary_rate": float(parsed.get("ordinary_rate", defaults["ordinary_rate"])),
        "ltcg_rate": float(parsed.get("ltcg_rate", defaults["ltcg_rate"])),
        "state_rate": float(parsed.get("state_rate", defaults["state_rate"])),
        "niit_rate": float(parsed.get("niit_rate", defaults["niit_rate"])),
    }
