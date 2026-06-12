# mypy: ignore-errors
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from dataclasses import asdict, is_dataclass

from ..exceptions import DataValidationError, DuplicateThemeError, PersistenceError
from . import core

logger = core.logger
DEFAULT_OPERATING_MODE = core.DEFAULT_OPERATING_MODE
DEFAULT_AUTO_APPROVE_THRESHOLD = core.DEFAULT_AUTO_APPROVE_THRESHOLD
DEFAULT_DAILY_CAPITAL_CEILING_PCT = core.DEFAULT_DAILY_CAPITAL_CEILING_PCT
LOT_SELECTION_METHODS = core.LOT_SELECTION_METHODS
DEFAULT_LOT_SELECTION_METHOD = core.DEFAULT_LOT_SELECTION_METHOD
DEFAULT_LTCG_DEFER_WINDOW_DAYS = core.DEFAULT_LTCG_DEFER_WINDOW_DAYS
OPERATING_MODES = core.OPERATING_MODES
ALERT_TYPES = core.ALERT_TYPES
NOTIFICATION_CHANNELS = core.NOTIFICATION_CHANNELS
_NOTIFICATION_DEFAULT_MATRIX = core._NOTIFICATION_DEFAULT_MATRIX
_SECTOR_CACHE_TTL_DAYS = core._SECTOR_CACHE_TTL_DAYS
_EARNINGS_CACHE_TTL_HOURS = core._EARNINGS_CACHE_TTL_HOURS


def _connect():
    return core._connect()


def upsert_thesis(ticker: str, thesis: str | None) -> str | None:
    with _connect() as conn:
        theme = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
        now = datetime.now(timezone.utc).isoformat()
        if theme is None:
            conn.execute(
                """
                INSERT INTO investment_theme (name, narrative, conviction, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("General", "Migrated from legacy per-ticker theses", 3, "Active", now, now),
            )
            theme = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
        theme_id = int(theme["id"])
        row = conn.execute(
            "SELECT rationale FROM theme_ticker WHERE theme_id = ? AND ticker = ?",
            (theme_id, ticker.upper()),
        ).fetchone()
        existing = str(row["rationale"]) if row and row["rationale"] else None
        if thesis is None:
            return existing
        conn.execute(
            """
            INSERT INTO theme_ticker (theme_id, ticker, role, rationale, time_horizon, added_at, updated_at)
            VALUES (?, ?, 'Core', ?, 'strategic', ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET rationale = excluded.rationale, updated_at = excluded.updated_at
            """,
            (theme_id, ticker.upper(), thesis.strip(), now, now),
        )
        return thesis.strip()


def delete_thesis(ticker: str) -> bool:
    with _connect() as conn:
        theme = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
        if theme is None:
            return False
        cursor = conn.execute("DELETE FROM theme_ticker WHERE theme_id = ? AND ticker = ?", (int(theme["id"]), ticker.upper()))
        return bool(cursor.rowcount)


def list_theses() -> list[dict[str, str]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, rationale AS thesis, updated_at
            FROM theme_ticker
            WHERE theme_id = (SELECT id FROM investment_theme WHERE name = 'General')
            ORDER BY ticker ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]


def create_theme(name: str, narrative: str = "", conviction: int = 3, status: str = "Active", sector_hint: str = "") -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO investment_theme (name, narrative, sector_hint, conviction, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (name.strip(), narrative.strip(), sector_hint.strip(), int(conviction), status, now, now),
            )
        except sqlite3.IntegrityError as exc:
            if "UNIQUE constraint" in str(exc):
                raise DuplicateThemeError(f"A theme named '{name.strip()}' already exists.") from exc
            raise
        theme_id = int(cursor.lastrowid)
    return get_theme(theme_id) or {}


def update_theme(
    theme_id: int,
    *,
    name: str | None = None,
    narrative: str | None = None,
    sector_hint: str | None = None,
    conviction: int | None = None,
    status: str | None = None,
) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    if name is not None:
        updates.append("name = ?")
        params.append(name.strip())
    if narrative is not None:
        updates.append("narrative = ?")
        params.append(narrative.strip())
    if sector_hint is not None:
        updates.append("sector_hint = ?")
        params.append(sector_hint.strip())
    if conviction is not None:
        updates.append("conviction = ?")
        params.append(int(conviction))
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    if not updates:
        return get_theme(theme_id)
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(int(theme_id))
    with _connect() as conn:
        try:
            cursor = conn.execute(f"UPDATE investment_theme SET {', '.join(updates)} WHERE id = ?", params)
        except sqlite3.IntegrityError as exc:
            if "UNIQUE constraint" in str(exc):
                raise DuplicateThemeError("A theme with that name already exists.") from exc
            raise
        if not cursor.rowcount:
            return None
    return get_theme(theme_id)


def delete_theme(theme_id: int) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM investment_theme WHERE id = ?", (int(theme_id),))
        return bool(cursor.rowcount)


def get_theme_tickers(theme_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT theme_id, ticker, role, rationale, entry_price, target_price, stop_price, time_horizon, added_at, updated_at
            FROM theme_ticker
            WHERE theme_id = ?
            ORDER BY ticker ASC
            """,
            (int(theme_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def get_theme(theme_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, name, narrative, sector_hint, conviction, status, created_at, updated_at
            FROM investment_theme
            WHERE id = ?
            """,
            (int(theme_id),),
        ).fetchone()
    if row is None:
        return None
    theme = dict(row)
    theme["tickers"] = get_theme_tickers(int(theme_id))
    return theme


def list_themes(include_closed: bool = False) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, name, narrative, sector_hint, conviction, status, created_at, updated_at
            FROM investment_theme
            WHERE (? = 1 OR status != 'Closed')
            ORDER BY name ASC
            """,
            (1 if include_closed else 0,),
        ).fetchall()
    themes = [dict(row) for row in rows]
    for theme in themes:
        theme["tickers"] = get_theme_tickers(int(theme["id"]))
    return themes


def add_ticker_to_theme(
    theme_id: int,
    ticker: str,
    *,
    role: str = "Core",
    rationale: str = "",
    entry_price: float | None = None,
    target_price: float | None = None,
    stop_price: float | None = None,
    time_horizon: str = "strategic",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO theme_ticker (
                theme_id, ticker, role, rationale, entry_price, target_price, stop_price, time_horizon, added_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET
                role = excluded.role,
                rationale = excluded.rationale,
                entry_price = excluded.entry_price,
                target_price = excluded.target_price,
                stop_price = excluded.stop_price,
                time_horizon = excluded.time_horizon,
                updated_at = excluded.updated_at
            """,
            (
                int(theme_id),
                ticker.upper(),
                role,
                rationale.strip(),
                float(entry_price) if entry_price is not None else None,
                float(target_price) if target_price is not None else None,
                float(stop_price) if stop_price is not None else None,
                time_horizon,
                now,
                now,
            ),
        )
    return next((item for item in get_theme_tickers(theme_id) if str(item["ticker"]).upper() == ticker.upper()), {})


def remove_ticker_from_theme(theme_id: int, ticker: str) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM theme_ticker WHERE theme_id = ? AND ticker = ?", (int(theme_id), ticker.upper()))
        return bool(cursor.rowcount)


def update_ticker_in_theme(theme_id: int, ticker: str, **fields: Any) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    for key in ("role", "rationale", "entry_price", "target_price", "stop_price", "time_horizon"):
        if key in fields:
            value = fields[key]
            if key in {"entry_price", "target_price", "stop_price"} and value is not None:
                value = float(value)
            elif key == "rationale" and value is not None:
                value = str(value).strip()
            updates.append(f"{key} = ?")
            params.append(value)
    if not updates:
        return next((item for item in get_theme_tickers(theme_id) if str(item["ticker"]).upper() == ticker.upper()), None)
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.extend([int(theme_id), ticker.upper()])
    with _connect() as conn:
        cursor = conn.execute(f"UPDATE theme_ticker SET {', '.join(updates)} WHERE theme_id = ? AND ticker = ?", params)
        if not cursor.rowcount:
            return None
    return next((item for item in get_theme_tickers(theme_id) if str(item["ticker"]).upper() == ticker.upper()), None)


def get_ticker_themes(ticker: str) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                tt.theme_id,
                it.name AS theme_name,
                it.narrative,
                it.conviction,
                it.status,
                tt.ticker,
                tt.role,
                tt.rationale,
                tt.entry_price,
                tt.target_price,
                tt.stop_price,
                tt.time_horizon,
                tt.added_at,
                tt.updated_at
            FROM theme_ticker tt
            JOIN investment_theme it ON it.id = tt.theme_id
            WHERE tt.ticker = ?
            ORDER BY it.name ASC
            """,
            (ticker.upper(),),
        ).fetchall()
    return [dict(row) for row in rows]


def get_theme_health_data(theme_id: int) -> dict[str, Any]:
    theme = get_theme(theme_id)
    if theme is None:
        return {}
    return {"theme": theme, "tickers": theme.get("tickers", [])}


def save_supply_chain_layers(theme_id: int, layers: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    normalized_layers: list[tuple[str, str, str]] = []
    seen_layers: set[str] = set()
    for layer in layers:
        name = str((layer or {}).get("layer") or "").strip()
        if not name or name in seen_layers:
            continue
        seen_layers.add(name)
        normalized_layers.append(
            (
                name[:100],
                str((layer or {}).get("description") or "").strip()[:1000],
                str((layer or {}).get("example_companies") or "").strip()[:500],
            )
        )
    with _connect() as conn:
        conn.execute("DELETE FROM theme_supply_chain WHERE theme_id = ?", (int(theme_id),))
        for layer_name, description, companies in normalized_layers:
            conn.execute(
                """
                INSERT INTO theme_supply_chain (theme_id, layer, description, example_companies, generated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(theme_id), layer_name, description, companies, now),
            )
    return get_supply_chain(theme_id)


def get_supply_chain(theme_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, theme_id, layer, description, example_companies, generated_at
            FROM theme_supply_chain
            WHERE theme_id = ?
            ORDER BY layer ASC
            """,
            (int(theme_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def delete_supply_chain(theme_id: int) -> int:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM theme_supply_chain WHERE theme_id = ?", (int(theme_id),))
        return int(cursor.rowcount or 0)


def upsert_watchlist_candidate(
    theme_id: int,
    ticker: str,
    *,
    company_name: str = "",
    supply_chain_layer: str = "",
    discovery_rationale: str = "",
    suggested_role: str = "Critical-Path",
    suggested_entry_price: float | None = None,
    suggested_stop_price: float | None = None,
    crowd_score: int = 50,
    normalized_crowd_score: int | None = None,
    crowd_details: str = "",
    regime_label: str | None = None,
    regime_probability: float | None = None,
    status: str = "Watching",
    notes: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    ticker_key = ticker.upper()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO discovery_watchlist (
                theme_id, ticker, company_name, supply_chain_layer, discovery_rationale,
                suggested_role, suggested_entry_price, suggested_stop_price, crowd_score,
                normalized_crowd_score,
                crowd_details, regime_label, regime_probability, status, discovered_at,
                last_scanned_at, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET
                company_name = excluded.company_name,
                supply_chain_layer = excluded.supply_chain_layer,
                discovery_rationale = excluded.discovery_rationale,
                suggested_role = excluded.suggested_role,
                suggested_entry_price = excluded.suggested_entry_price,
                suggested_stop_price = excluded.suggested_stop_price,
                crowd_score = excluded.crowd_score,
                normalized_crowd_score = excluded.normalized_crowd_score,
                crowd_details = excluded.crowd_details,
                regime_label = excluded.regime_label,
                regime_probability = excluded.regime_probability,
                status = CASE
                    WHEN discovery_watchlist.status IN ('Added', 'Passed') THEN discovery_watchlist.status
                    ELSE excluded.status
                END,
                last_scanned_at = excluded.last_scanned_at,
                notes = CASE
                    WHEN discovery_watchlist.notes != '' THEN discovery_watchlist.notes
                    ELSE excluded.notes
                END
            """,
            (
                int(theme_id),
                ticker_key,
                company_name.strip(),
                supply_chain_layer.strip(),
                discovery_rationale.strip(),
                suggested_role,
                float(suggested_entry_price) if suggested_entry_price is not None else None,
                float(suggested_stop_price) if suggested_stop_price is not None else None,
                max(0, min(100, int(crowd_score))),
                max(0, min(100, int(normalized_crowd_score))) if normalized_crowd_score is not None else None,
                crowd_details,
                regime_label,
                float(regime_probability) if regime_probability is not None else None,
                status,
                now,
                now,
                notes.strip(),
            ),
        )
    row = get_watchlist_by_ticker(ticker_key)
    return next((item for item in row if int(item["theme_id"]) == int(theme_id)), {})


def get_watchlist(
    theme_id: int | None = None,
    status: str | list[str] | None = None,
    max_crowd_score: int | None = None,
) -> list[dict[str, Any]]:
    query = [
        """
        SELECT
            dw.*,
            it.name AS theme_name,
            it.conviction AS theme_conviction,
            it.status AS theme_status
        FROM discovery_watchlist dw
        JOIN investment_theme it ON it.id = dw.theme_id
        WHERE 1 = 1
        """
    ]
    params: list[Any] = []
    if theme_id is not None:
        query.append("AND dw.theme_id = ?")
        params.append(int(theme_id))
    if isinstance(status, str) and status:
        query.append("AND dw.status = ?")
        params.append(status)
    elif isinstance(status, list) and status:
        placeholders = ", ".join("?" for _ in status)
        query.append(f"AND dw.status IN ({placeholders})")
        params.extend(status)
    else:
        query.append("AND dw.status NOT IN ('Expired', 'Passed')")
    if max_crowd_score is not None:
        query.append("AND dw.crowd_score <= ?")
        params.append(int(max_crowd_score))
    query.append("ORDER BY CASE dw.status WHEN 'Entry Signal' THEN 0 WHEN 'Watching' THEN 1 WHEN 'Added' THEN 2 WHEN 'Passed' THEN 3 ELSE 4 END, dw.crowd_score ASC, dw.ticker ASC")
    with _connect() as conn:
        rows = conn.execute("\n".join(query), params).fetchall()
    return [dict(row) for row in rows]


def get_watchlist_entry(watchlist_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
                dw.*,
                it.name AS theme_name,
                it.conviction AS theme_conviction,
                it.status AS theme_status
            FROM discovery_watchlist dw
            JOIN investment_theme it ON it.id = dw.theme_id
            WHERE dw.id = ?
            """,
            (int(watchlist_id),),
        ).fetchone()
    return dict(row) if row else None


def update_watchlist_status(watchlist_id: int, status: str, **kwargs: Any) -> dict[str, Any] | None:
    updates = ["status = ?"]
    params: list[Any] = [status]
    for key in ("notes", "regime_label", "regime_probability", "crowd_score", "crowd_details", "suggested_entry_price", "suggested_stop_price", "last_scanned_at"):
        if key in kwargs:
            updates.append(f"{key} = ?")
            params.append(kwargs[key])
    if status == "Entry Signal":
        updates.append("entry_signal_at = ?")
        params.append(kwargs.get("entry_signal_at") or datetime.now(timezone.utc).isoformat())
    params.append(int(watchlist_id))
    with _connect() as conn:
        cursor = conn.execute(
            f"UPDATE discovery_watchlist SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        if not cursor.rowcount:
            return None
    return get_watchlist_entry(watchlist_id)


def update_watchlist_fundamental_gate(
    watchlist_id: int,
    *,
    passed: bool,
    piotroski_score: int | None,
    roic_pct: float | None,
    altman_z_score: float | None = None,
    altman_z_interpretation: str = "",
    details: Any = None,
) -> None:
    """Persist fundamental gate diagnostics on a discovery watchlist row."""
    details_json = ""
    if details is not None:
        try:
            details_json = json.dumps(asdict(details) if is_dataclass(details) else details, default=str)
        except Exception:
            details_json = ""
    with _connect() as conn:
        conn.execute(
            """
            UPDATE discovery_watchlist
            SET fundamental_gate_passed = ?,
                piotroski_score = ?,
                roic_pct = ?,
                altman_z_score = ?,
                altman_z_interpretation = ?,
                fundamental_details = ?
            WHERE id = ?
            """,
            (
                1 if passed else 0,
                int(piotroski_score) if piotroski_score is not None else None,
                float(roic_pct) if roic_pct is not None else None,
                float(altman_z_score) if altman_z_score is not None else None,
                str(altman_z_interpretation or ""),
                details_json,
                int(watchlist_id),
            ),
        )


def update_watchlist_cross_sectional(
    watchlist_id: int,
    *,
    beta: float | None = None,
    beta_adjusted_return: float | None = None,
    vol_z_score: float | None = None,
    vol_z_interpretation: str = "",
    normalized_crowd_score: int | None = None,
    peer_percentile_json: str = "",
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE discovery_watchlist
            SET beta = ?,
                beta_adjusted_return = ?,
                vol_z_score = ?,
                vol_z_interpretation = ?,
                normalized_crowd_score = ?,
                peer_percentile_json = ?
            WHERE id = ?
            """,
            (
                float(beta) if beta is not None else None,
                float(beta_adjusted_return) if beta_adjusted_return is not None else None,
                float(vol_z_score) if vol_z_score is not None else None,
                str(vol_z_interpretation or ""),
                int(normalized_crowd_score) if normalized_crowd_score is not None else None,
                str(peer_percentile_json or ""),
                int(watchlist_id),
            ),
        )


def get_watchlist_by_ticker(ticker: str) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                dw.*,
                it.name AS theme_name,
                it.conviction AS theme_conviction,
                it.status AS theme_status
            FROM discovery_watchlist dw
            JOIN investment_theme it ON it.id = dw.theme_id
            WHERE dw.ticker = ?
            ORDER BY it.name ASC
            """,
            (ticker.upper(),),
        ).fetchall()
    return [dict(row) for row in rows]


def delete_watchlist_entry(watchlist_id: int) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM discovery_watchlist WHERE id = ?", (int(watchlist_id),))
        return bool(cursor.rowcount)


def get_watchlist_stats() -> dict[str, Any]:
    with _connect() as conn:
        status_rows = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM discovery_watchlist
            GROUP BY status
            ORDER BY status ASC
            """
        ).fetchall()
        theme_rows = conn.execute(
            """
            SELECT it.name AS theme_name, COUNT(*) AS count, AVG(dw.crowd_score) AS avg_crowd_score
            FROM discovery_watchlist dw
            JOIN investment_theme it ON it.id = dw.theme_id
            GROUP BY dw.theme_id, it.name
            ORDER BY count DESC, it.name ASC
            """
        ).fetchall()
        summary = conn.execute(
            """
            SELECT COUNT(*) AS total, AVG(crowd_score) AS avg_crowd_score
            FROM discovery_watchlist
            """
        ).fetchone()
    return {
        "total": int(summary["total"] or 0) if summary else 0,
        "avg_crowd_score": float(summary["avg_crowd_score"]) if summary and summary["avg_crowd_score"] is not None else None,
        "by_status": {str(row["status"]): int(row["count"] or 0) for row in status_rows},
        "by_theme": [dict(row) for row in theme_rows],
    }


def save_regime_event(ticker: str, label: str, state_id: int) -> dict[str, int | str | None]:
    now = datetime.now(timezone.utc)
    now_text = now.isoformat()
    with _connect() as conn:
        row = conn.execute(
            "SELECT current_label, current_state_id, changed_at FROM regime_events WHERE ticker = ?",
            (ticker.upper(),),
        ).fetchone()
        previous_label = row["current_label"] if row else None
        changed_at = row["changed_at"] if row else now_text

        if row and row["current_state_id"] == state_id:
            conn.execute(
                "UPDATE regime_events SET updated_at = ? WHERE ticker = ?",
                (now_text, ticker.upper()),
            )
        else:
            changed_at = now_text
            conn.execute(
                """
                INSERT INTO regime_events (ticker, current_label, current_state_id, changed_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    current_label = excluded.current_label,
                    current_state_id = excluded.current_state_id,
                    changed_at = excluded.changed_at,
                    updated_at = excluded.updated_at
                """,
                (ticker.upper(), label, state_id, changed_at, now_text),
            )
            if row:
                conn.execute(
                    """
                    INSERT INTO regime_change_history (ticker, previous_label, current_label, current_state_id, changed_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (ticker.upper(), previous_label, label, state_id, changed_at),
                )

    changed_dt = datetime.fromisoformat(changed_at)
    # Backwards-compatible persistence metadata only; regime_days from the HMM engine
    # is the authoritative regime age shown in the dashboard and CLI.
    days_in_regime = max(0, (now - changed_dt).days)
    return {"previous_label": previous_label, "days_in_regime": days_in_regime}


def save_regime_change_with_price(
    ticker: str,
    previous_label: str | None,
    current_label: str,
    state_id: int,
    price: float | None,
) -> int:
    now_text = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO regime_change_history (
                ticker,
                previous_label,
                current_label,
                current_state_id,
                changed_at,
                price_at_change
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ticker.upper(), previous_label, current_label, int(state_id), now_text, price),
        )
        return int(cursor.lastrowid)


def save_sentiment(ticker: str, score: int, sentiment: str, catalyst_count: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sentiment_history (ticker, score, sentiment, catalyst_count, recorded_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker.upper(), int(score), sentiment, int(catalyst_count), now),
        )


def get_sentiment_history(ticker: str, days: int = 30) -> list[dict[str, int | str]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, score, sentiment, catalyst_count, recorded_at
            FROM sentiment_history
            WHERE ticker = ?
              AND recorded_at >= datetime('now', ?)
            ORDER BY recorded_at ASC
            """,
            (ticker.upper(), f"-{int(days)} day"),
        ).fetchall()
    return [dict(row) for row in rows]


def get_recent_regime_changes(ticker: str, days: int = 7) -> list[dict[str, int | str | None]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, previous_label, current_label, current_state_id, changed_at
            FROM regime_change_history
            WHERE ticker = ?
              AND changed_at >= datetime('now', ?)
            ORDER BY changed_at DESC
            """,
            (ticker.upper(), f"-{int(days)} day"),
        ).fetchall()
    return [dict(row) for row in rows]


def get_pending_transition_outcomes(
    *,
    lookback_days: int = 90,
    as_of: str | None = None,
) -> list[dict[str, Any]]:
    now_text = as_of or datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ticker, changed_at, price_at_change
            FROM regime_change_history
            WHERE changed_at >= datetime(?, ?)
              AND (
                (return_5d IS NULL AND changed_at <= datetime(?, '-5 day'))
                OR (return_10d IS NULL AND changed_at <= datetime(?, '-10 day'))
                OR (return_21d IS NULL AND changed_at <= datetime(?, '-21 day'))
              )
            ORDER BY changed_at ASC
            """,
            (now_text, f"-{int(lookback_days)} day", now_text, now_text, now_text),
        ).fetchall()
    return [dict(row) for row in rows]


def update_transition_outcome(
    change_id: int,
    *,
    return_5d: float | None = None,
    return_10d: float | None = None,
    return_21d: float | None = None,
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE regime_change_history
            SET return_5d = COALESCE(?, return_5d),
                return_10d = COALESCE(?, return_10d),
                return_21d = COALESCE(?, return_21d),
                outcome_updated_at = ?
            WHERE id = ?
            """,
            (
                return_5d,
                return_10d,
                return_21d,
                datetime.now(timezone.utc).isoformat(),
                int(change_id),
            ),
        )


def get_transition_journal(ticker: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    with _connect() as conn:
        if ticker:
            rows = conn.execute(
                """
                SELECT *
                FROM regime_change_history
                WHERE ticker = ?
                ORDER BY changed_at DESC
                LIMIT ?
                """,
                (ticker.upper(), int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM regime_change_history
                ORDER BY changed_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    return [dict(row) for row in rows]


def get_transition_statistics() -> dict[str, Any]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(previous_label, 'Unknown') AS previous_label,
                current_label,
                AVG(return_5d) AS avg_return_5d,
                AVG(return_10d) AS avg_return_10d,
                AVG(return_21d) AS avg_return_21d,
                AVG(CASE WHEN return_5d > 0 THEN 1.0 WHEN return_5d IS NULL THEN NULL ELSE 0.0 END) AS hit_rate_5d,
                AVG(CASE WHEN return_10d > 0 THEN 1.0 WHEN return_10d IS NULL THEN NULL ELSE 0.0 END) AS hit_rate_10d,
                AVG(CASE WHEN return_21d > 0 THEN 1.0 WHEN return_21d IS NULL THEN NULL ELSE 0.0 END) AS hit_rate_21d,
                COUNT(*) AS count
            FROM regime_change_history
            GROUP BY COALESCE(previous_label, 'Unknown'), current_label
            ORDER BY count DESC, previous_label ASC, current_label ASC
            """
        ).fetchall()
    pairs = []
    for row in rows:
        item = dict(row)
        item["transition"] = f"{item['previous_label']}→{item['current_label']}"
        pairs.append(item)
    return {"rows": pairs}


def get_latest_regime_label(ticker: str, as_of: str) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT current_label
            FROM regime_change_history
            WHERE ticker = ? AND changed_at <= ?
            ORDER BY changed_at DESC
            LIMIT 1
            """,
            (str(ticker or "").upper(), str(as_of)),
        ).fetchone()
    return str(row["current_label"]) if row and row["current_label"] else None


def get_cached_sector(ticker: str, *, max_age_days: int = _SECTOR_CACHE_TTL_DAYS) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT sector
            FROM sector_cache
            WHERE ticker = ?
              AND cached_at >= datetime('now', ?)
            """,
            (ticker.upper(), f"-{int(max_age_days)} day"),
        ).fetchone()
    return str(row["sector"]) if row and row["sector"] else None


def save_sector_cache(ticker: str, sector: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sector_cache (ticker, sector, cached_at)
            VALUES (?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                sector = excluded.sector,
                cached_at = excluded.cached_at
            """,
            (ticker.upper(), sector, now),
        )


def get_cached_earnings_date(ticker: str, *, max_age_hours: int = _EARNINGS_CACHE_TTL_HOURS) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT earnings_date
            FROM earnings_cache
            WHERE ticker = ?
              AND cached_at >= datetime('now', ?)
            """,
            (ticker.upper(), f"-{int(max_age_hours)} hour"),
        ).fetchone()
    if row is None:
        return None
    return str(row["earnings_date"]) if row["earnings_date"] else None


def save_earnings_cache(ticker: str, earnings_date: str | None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO earnings_cache (ticker, earnings_date, cached_at)
            VALUES (?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                earnings_date = excluded.earnings_date,
                cached_at = excluded.cached_at
            """,
            (ticker.upper(), earnings_date, now),
        )


def get_historical_regime_durations(ticker: str | None = None) -> dict[str, Any]:
    with _connect() as conn:
        if ticker:
            rows = conn.execute(
                """
                SELECT ticker, current_label, changed_at
                FROM regime_change_history
                WHERE ticker = ?
                ORDER BY changed_at ASC
                """,
                (ticker.upper(),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ticker, current_label, changed_at
                FROM regime_change_history
                ORDER BY ticker ASC, changed_at ASC
                """
            ).fetchall()

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        grouped.setdefault(str(item["ticker"]).upper(), []).append(item)

    result: dict[str, Any] = {}
    for ticker_key, ticker_rows in grouped.items():
        per_label: dict[str, list[float]] = {}
        for current, nxt in zip(ticker_rows, ticker_rows[1:]):
            label = str(current.get("current_label") or "")
            if not label:
                continue
            start = datetime.fromisoformat(str(current["changed_at"]))
            end = datetime.fromisoformat(str(nxt["changed_at"]))
            duration_days = max(0.0, (end - start).total_seconds() / 86400.0)
            per_label.setdefault(label, []).append(duration_days)
        label_stats: dict[str, Any] = {}
        for label, values in per_label.items():
            ordered = sorted(values)
            if not ordered:
                continue
            count = len(ordered)
            mid = count // 2
            median = ordered[mid] if count % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0
            label_stats[label] = {
                "count": count,
                "avg": sum(ordered) / count,
                "median": median,
                "min": ordered[0],
                "max": ordered[-1],
            }
        result[ticker_key] = label_stats
    return result if ticker is None else result.get(ticker.upper(), {})
