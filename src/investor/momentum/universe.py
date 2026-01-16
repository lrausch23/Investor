from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from sqlalchemy.orm import Session

from src.core.external_holdings import build_holdings_view
from src.db.models import UniverseMembership, WatchlistItem
from src.investor.momentum.utils import normalize_ticker, parse_ticker_list


@dataclass(frozen=True)
class UniverseResult:
    key: str
    label: str
    tickers: list[str]
    warning: str | None = None


def list_universe_options() -> list[dict[str, str]]:
    return [
        {"key": "sp500", "label": "S&P 500 (SP500)"},
        {"key": "nasdaq100", "label": "Nasdaq 100"},
        {"key": "watchlist", "label": "Watchlist"},
        {"key": "holdings", "label": "Holdings"},
        {"key": "custom", "label": "Custom list"},
    ]


def _from_membership(session: Session, universe_db_key: str, *, key: str, label: str) -> UniverseResult:
    u = universe_db_key.strip().upper()
    rows = session.query(UniverseMembership.ticker).filter(UniverseMembership.universe == u).order_by(UniverseMembership.ticker.asc()).all()
    tickers = [normalize_ticker(t) for (t,) in rows]
    tickers = [t for t in tickers if t]
    tickers = list(dict.fromkeys(tickers))
    warn = None
    if not tickers:
        warn = f"No tickers loaded for {label}. Import constituents/classification to enable this universe."
    return UniverseResult(key=key, label=label, tickers=tickers, warning=warn)


def get_universe(
    session: Session,
    *,
    universe: str,
    custom_list: str = "",
) -> UniverseResult:
    key = (universe or "").strip().lower() or "sp500"
    if key == "sp500":
        return _from_membership(session, "SP500", key="sp500", label="S&P 500")
    if key == "nasdaq100":
        return _from_membership(session, "NASDAQ100", key="nasdaq100", label="Nasdaq 100")
    if key == "watchlist":
        rows = session.query(WatchlistItem.ticker).order_by(WatchlistItem.created_at.desc()).all()
        tickers = [normalize_ticker(t) for (t,) in rows]
        tickers = [t for t in tickers if t]
        tickers = list(dict.fromkeys(tickers))
        warn = None if tickers else "Watchlist is empty. Add tickers from a Sector page."
        return UniverseResult(key=key, label="Watchlist", tickers=tickers, warning=warn)
    if key == "holdings":
        view = build_holdings_view(session=session, scope="household", account_id=None)
        tickers: list[str] = []
        for p in view.positions:
            t = normalize_ticker(p.symbol or "")
            # Skip synthetic cash symbols.
            if not t or t.startswith("CASH"):
                continue
            if ":" in t:
                # e.g. CASH:USD, etc.
                continue
            tickers.append(t)
        tickers = list(dict.fromkeys(tickers))
        warn = None if tickers else "No equity tickers found in latest holdings snapshot."
        return UniverseResult(key=key, label="Holdings", tickers=tickers, warning=warn)
    if key == "custom":
        tickers = parse_ticker_list(custom_list)
        warn = None if tickers else "Enter one or more tickers (comma/space-separated)."
        return UniverseResult(key=key, label="Custom list", tickers=tickers, warning=warn)
    # Fallback
    return UniverseResult(key="sp500", label="S&P 500", tickers=[], warning="Unknown universe; select a supported universe.")
