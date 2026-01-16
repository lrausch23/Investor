from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import or_
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.dashboard_service import DashboardScope, parse_scope
from src.core.connection_preference import preferred_active_connection_ids_for_taxpayers
from src.db.models import (
    Account,
    BullionHolding,
    CashBalance,
    ExternalAccountMap,
    ExternalConnection,
    ExternalHoldingSnapshot,
    ExternalTransactionMap,
    PositionLot,
    Security,
    TaxLot,
    TaxpayerEntity,
    Transaction,
)
from src.core.portfolio import latest_cash_by_account


@dataclass(frozen=True)
class HoldingPosition:
    account_id: Optional[int]
    account_name: str
    taxpayer_type: Optional[str]
    symbol: str
    qty: Optional[float]
    latest_price: Optional[float]
    latest_price_as_of: Optional[dt.date]
    latest_price_fetched_at: Optional[dt.datetime]
    market_value: Optional[float]
    market_value_snapshot: Optional[float]
    cost_basis_total: Optional[float]
    pnl_amount: Optional[float]
    pnl_pct: Optional[float]
    tax_status: Optional[str]
    entered_date: Optional[dt.date]
    wash_safe_exit_date: Optional[dt.date]
    as_of: Optional[dt.datetime]


@dataclass(frozen=True)
class HoldingsView:
    scope: DashboardScope
    scope_label: str
    account_id: Optional[int]
    account_name: str
    as_of: Optional[dt.datetime]
    positions: list[HoldingPosition]
    total_value: float
    total_market_value: float
    total_initial_cost: Optional[float]
    total_pnl_amount: Optional[float]
    avg_pnl_pct: Optional[float]
    totals_missing_cost_count: int
    cash_total: float
    cash_asof: Optional[dt.date]
    ytd_start_value: Optional[float]
    ytd_start_asof: Optional[dt.datetime]
    gain_begin_value: Optional[float]
    gain_begin_label: str
    ytd_contributions: float
    ytd_withdrawals: float
    ytd_dividends_received: float  # dividends + interest (cash received)
    ytd_dividends: float
    ytd_interest_net: float
    ytd_interest_received: float
    ytd_interest_paid: float
    ytd_withholding: float
    ytd_fees: float
    ytd_net_cashflow: float
    ytd_gain_value: Optional[float]
    ytd_return_pct: Optional[float]
    recent_cashflows: list[dict[str, Any]]
    ytd_transfers: list[dict[str, Any]]
    ytd_deposit_count: int
    ytd_withdrawal_count: int
    pnl_planning_value: float
    pnl_return_on_cost: Optional[float]
    warnings: list[str]
    data_sources: list[dict[str, Any]]


def _scope_label(scope: DashboardScope) -> str:
    if scope == "trust":
        return "Trust only"
    if scope == "personal":
        return "Personal only"
    return "Household"


def _latest_price_from_csv(*, path: Path, symbol: str, as_of: dt.date) -> tuple[dt.date, float] | None:
    try:
        from portfolio_report.prices import load_price_csv
    except Exception:
        return None
    try:
        series = load_price_csv(path, symbol)
    except Exception:
        return None
    for d, px in reversed(series.points or []):
        if d <= as_of and px and float(px) > 0:
            return d, float(px)
    return None


def _read_price_fetched_at(csv_path: Path) -> dt.datetime | None:
    """
    Best-effort 'when was this price file last refreshed' timestamp.

    - Prefers a JSON sidecar `{TICKER}.json` (used by the yfinance cache).
    - Falls back to file mtime (UTC).
    """
    try:
        meta_p = csv_path.with_suffix(".json")
        if meta_p.exists():
            meta = json.loads(meta_p.read_text(encoding="utf-8"))
            qt = str((meta or {}).get("quote_time") or "").strip()
            if qt:
                try:
                    return dt.datetime.fromisoformat(qt.replace("Z", "+00:00"))
                except Exception:
                    pass
            fetched = str((meta or {}).get("fetched_at") or "").strip()
            if fetched:
                return dt.datetime.fromisoformat(fetched.replace("Z", "+00:00"))
    except Exception:
        pass
    try:
        return dt.datetime.fromtimestamp(csv_path.stat().st_mtime, tz=dt.timezone.utc)
    except Exception:
        return None


def _latest_prices_for_symbols(
    *,
    prices_dir: Path,
    symbols: list[str],
    as_of: dt.date,
    base_currency: str = "USD",
) -> dict[str, tuple[dt.date, float, dt.datetime | None]]:
    out: dict[str, tuple[dt.date, float, dt.datetime | None]] = {}
    prices_dir = Path(prices_dir)
    yfinance_dir = prices_dir / "yfinance"
    try:
        from market_data.symbols import normalize_ticker, sanitize_ticker
    except Exception:
        normalize_ticker = None  # type: ignore[assignment]
        sanitize_ticker = None  # type: ignore[assignment]

    for raw in symbols:
        sym = (raw or "").strip().upper()
        if not sym or sym in out:
            continue

        candidates: list[Path] = []
        p = prices_dir / f"{sym}.csv"
        if p.exists():
            candidates.append(p)

        provider_ticker = None
        if normalize_ticker is not None:
            try:
                ns = normalize_ticker(sym, base_currency=base_currency)  # type: ignore[misc]
                provider_ticker = getattr(ns, "provider_ticker", None)
            except Exception:
                provider_ticker = None
        if provider_ticker:
            p2 = prices_dir / f"{str(provider_ticker).upper()}.csv"
            if p2.exists() and p2 not in candidates:
                candidates.append(p2)

        if sanitize_ticker is not None:
            y1 = yfinance_dir / f"{sanitize_ticker(sym)}.csv"  # type: ignore[misc]
            if y1.exists() and y1 not in candidates:
                candidates.append(y1)
            if provider_ticker:
                y2 = yfinance_dir / f"{sanitize_ticker(str(provider_ticker))}.csv"  # type: ignore[misc]
                if y2.exists() and y2 not in candidates:
                    candidates.append(y2)

        for cp in candidates:
            hit = _latest_price_from_csv(path=cp, symbol=sym, as_of=as_of)
            if hit is not None:
                d, px = hit
                out[sym] = (d, px, _read_price_fetched_at(cp))
                break

    return out


def accounts_with_snapshot_positions(session: Session, *, scope: str | DashboardScope) -> set[int]:
    """
    Returns internal account_ids that currently have at least one position in the latest holdings snapshots
    for ACTIVE connections within the given scope.

    Used to make the Holdings page portfolio selector less confusing when "placeholder" accounts exist.
    """
    sc: DashboardScope = parse_scope(scope if isinstance(scope, str) else scope)

    conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
    if sc == "trust":
        conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
    elif sc == "personal":
        conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
    connections = conn_q.filter(ExternalConnection.status == "ACTIVE").all()
    if connections:
        preferred_ids = preferred_active_connection_ids_for_taxpayers(
            session, taxpayer_ids=[c.taxpayer_entity_id for c in connections]
        )
        if preferred_ids:
            connections = [c for c in connections if int(c.id) in preferred_ids]
    conn_ids = [int(c.id) for c in connections]
    if not conn_ids:
        return set()

    # Pick the latest snapshot per connection by (as_of desc, id desc).
    # Using max(id) alone breaks when importing historical statement snapshots after live holdings snapshots:
    # the statement rows would "win" even though their as_of is older.
    snapshots: list[tuple[ExternalHoldingSnapshot, ExternalConnection]] = []
    rows = (
        session.query(ExternalHoldingSnapshot, ExternalConnection)
        .join(ExternalConnection, ExternalConnection.id == ExternalHoldingSnapshot.connection_id)
        .filter(ExternalHoldingSnapshot.connection_id.in_(conn_ids))
        .order_by(ExternalHoldingSnapshot.as_of.desc(), ExternalHoldingSnapshot.id.desc())
        .all()
    )
    seen_conn_ids: set[int] = set()
    for snap, conn in rows:
        cid = int(conn.id)
        if cid in seen_conn_ids:
            continue
        seen_conn_ids.add(cid)
        snapshots.append((snap, conn))
        if len(seen_conn_ids) >= len(conn_ids):
            break

    maps = session.query(ExternalAccountMap).filter(ExternalAccountMap.connection_id.in_(conn_ids)).all()
    map_by_conn_provider: dict[tuple[int, str], int] = {(m.connection_id, m.provider_account_id): m.account_id for m in maps}

    out: set[int] = set()
    for snap, conn in snapshots:
        payload = snap.payload_json or {}
        items = payload.get("items") or []
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            provider_acct = str(it.get("provider_account_id") or "").strip()
            symbol = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
            if not provider_acct or not symbol:
                continue
            # Some adapters store a synthetic "TOTAL" row (is_total=true) to represent statement/account valuation.
            # This is used for performance reporting and must not be treated as a position row.
            if bool(it.get("is_total")):
                acct_id = map_by_conn_provider.get((int(conn.id), provider_acct))
                if acct_id is not None:
                    out.add(int(acct_id))
                continue
            if symbol.startswith("CASH:"):
                continue
            acct_id = map_by_conn_provider.get((int(conn.id), provider_acct))
            if acct_id is not None:
                out.add(int(acct_id))
    return out


def build_holdings_view(
    session: Session,
    *,
    scope: str | DashboardScope,
    account_id: int | None = None,
    today: dt.date | None = None,
    prices_dir: Path | None = None,
) -> HoldingsView:
    sc: DashboardScope = parse_scope(scope if isinstance(scope, str) else scope)
    today_d = today or dt.date.today()
    prices_root = Path(prices_dir) if prices_dir is not None else None

    # Accounts included in scope.
    acct_q = session.query(Account, TaxpayerEntity).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    if sc == "trust":
        acct_q = acct_q.filter(TaxpayerEntity.type == "TRUST")
    elif sc == "personal":
        acct_q = acct_q.filter(TaxpayerEntity.type == "PERSONAL")
    accounts = acct_q.order_by(Account.name).all()
    acct_by_id = {a.id: (a, tp) for a, tp in accounts}

    if account_id is not None and account_id not in acct_by_id:
        account_id = None

    included_account_ids = [account_id] if account_id is not None else [a.id for a, _tp in accounts]

    # Wash scope accounts (wash applies within taxpayer entity):
    if account_id is not None and account_id in acct_by_id:
        wash_taxpayer_ids = {acct_by_id[account_id][0].taxpayer_entity_id}
    elif sc == "trust":
        wash_taxpayer_ids = {tp.id for _a, tp in accounts if tp.type == "TRUST"}
    elif sc == "personal":
        wash_taxpayer_ids = {tp.id for _a, tp in accounts if tp.type == "PERSONAL"}
    else:
        wash_taxpayer_ids = {tp.id for _a, tp in accounts}
    # Wash-sale reminder logic:
    # - A loss sale in a taxable account can be washed by buys in any account under the same taxpayer entity,
    #   including IRA accounts (which can make the loss permanently non-deductible).
    # - For safety, we consider BUY activity across all accounts in-scope for the taxpayer entity.
    wash_account_ids = [a.id for a, _tp in accounts if a.taxpayer_entity_id in wash_taxpayer_ids]

    # Latest holdings snapshot per connection (within scope).
    conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
    if sc == "trust":
        conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
    elif sc == "personal":
        conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
    connections = conn_q.filter(ExternalConnection.status == "ACTIVE").all()
    if connections:
        # Prefer IB_FLEX_WEB per taxpayer if present (avoids double-counting from offline+web).
        preferred_ids = preferred_active_connection_ids_for_taxpayers(
            session, taxpayer_ids=[c.taxpayer_entity_id for c in connections]
        )
        if preferred_ids:
            connections = [c for c in connections if int(c.id) in preferred_ids]
    conn_by_id = {int(c.id): c for c in connections}
    conn_ids = list(conn_by_id.keys())  # for cashflows/transactions filtering
    holdings_conn_ids = list(conn_by_id.keys())  # for holdings snapshots (may be narrowed below)

    snapshots: list[tuple[ExternalHoldingSnapshot, ExternalConnection]] = []
    # Preload external account maps for included connections.
    maps = (
        session.query(ExternalAccountMap)
        .filter(
            ExternalAccountMap.connection_id.in_(holdings_conn_ids),
            ExternalAccountMap.account_id.in_(included_account_ids),
        )
        .all()
        if holdings_conn_ids and included_account_ids
        else []
    )
    # For portfolio-scoped views, ignore connections that have no mapped accounts in-scope.
    if holdings_conn_ids and included_account_ids:
        mapped_conn_ids = {int(m.connection_id) for m in maps if m.connection_id is not None}
        holdings_conn_ids = [int(cid) for cid in holdings_conn_ids if int(cid) in mapped_conn_ids]

    map_by_conn_provider: dict[tuple[int, str], int] = {(int(m.connection_id), m.provider_account_id): int(m.account_id) for m in maps}

    if holdings_conn_ids:
        def _has_any_position_items(payload: dict[str, Any]) -> bool:
            items = payload.get("items") or []
            if not isinstance(items, list):
                return False
            for it in items:
                if not isinstance(it, dict):
                    continue
                if bool(it.get("is_total")):
                    continue
                sym = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
                if not sym:
                    continue
                if sym.startswith("CASH:"):
                    continue
                return True
            return False

        # Pick the latest snapshot per connection by (as_of desc, id desc), preferring a snapshot that actually
        # contains positions-by-ticker. This prevents valuation-only statement snapshots (TOTAL rows) from causing
        # the Holdings page to appear empty when a slightly older positions snapshot exists.
        # Using max(id) alone can select an older-dated statement snapshot inserted later, which would make
        # the Holdings page appear to "go back in time" after uploading baselines for performance.
        rows = (
            session.query(ExternalHoldingSnapshot, ExternalConnection)
            .join(ExternalConnection, ExternalConnection.id == ExternalHoldingSnapshot.connection_id)
            .filter(ExternalHoldingSnapshot.connection_id.in_(holdings_conn_ids))
            .order_by(ExternalHoldingSnapshot.as_of.desc(), ExternalHoldingSnapshot.id.desc())
            .all()
        )
        chosen_by_conn: dict[int, tuple[ExternalHoldingSnapshot, ExternalConnection]] = {}
        fallback_by_conn: dict[int, tuple[ExternalHoldingSnapshot, ExternalConnection]] = {}
        for snap, conn in rows:
            cid = int(conn.id)
            if cid in chosen_by_conn:
                continue
            if cid not in fallback_by_conn:
                fallback_by_conn[cid] = (snap, conn)
            payload = snap.payload_json or {}
            if _has_any_position_items(payload):
                chosen_by_conn[cid] = (snap, conn)
            if len(chosen_by_conn) >= len(holdings_conn_ids):
                break

        for cid in holdings_conn_ids:
            pick = chosen_by_conn.get(int(cid)) or fallback_by_conn.get(int(cid))
            if pick is not None:
                snapshots.append(pick)

    warnings: list[str] = []
    seen_sources_by_account: dict[int, set[str]] = {}
    cash_from_snapshot_by_account: dict[int, float] = {}
    data_sources: list[dict[str, Any]] = []

    raw_positions: list[HoldingPosition] = []
    max_as_of: Optional[dt.datetime] = None

    for snap, conn in snapshots:
        source_used = False
        payload = snap.payload_json or {}
        items = payload.get("items") or []
        if not isinstance(items, list):
            continue
        # Defensive de-duplication: when a connection uses multiple queries, IB Flex Web may include the same
        # open-positions section in multiple reports. Snapshots may therefore contain repeated
        # (provider_account_id, symbol) rows. We treat open positions as already-aggregated per symbol and
        # ignore duplicates to prevent double-counting.
        seen_item_keys: set[tuple[str, str]] = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            provider_acct = str(it.get("provider_account_id") or "").strip()
            symbol = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
            if not symbol:
                continue
            acct_id = map_by_conn_provider.get((int(conn.id), provider_acct))
            if acct_id is None or int(acct_id) not in included_account_ids:
                continue
            source_used = True
            # Synthetic statement/account totals are used for valuation snapshots; do not display as a position
            # or include it in position aggregation (otherwise totals are double-counted).
            if bool(it.get("is_total")):
                continue
            k = (provider_acct, symbol)
            if k in seen_item_keys:
                continue
            seen_item_keys.add(k)
            # Cash is surfaced via CashBalance; use snapshot cash only as fallback.
            if symbol.startswith("CASH:"):
                try:
                    cash_from_snapshot_by_account[int(acct_id)] = float(cash_from_snapshot_by_account.get(int(acct_id)) or 0.0) + float(it.get("market_value") or it.get("qty") or 0.0)
                except Exception:
                    pass
                continue

            acct_name = provider_acct or "Unmapped"
            taxpayer_type = None
            if acct_id is not None and acct_id in acct_by_id:
                acct_name = acct_by_id[acct_id][0].name
                taxpayer_type = acct_by_id[acct_id][1].type

            if acct_id is not None:
                seen_sources_by_account.setdefault(acct_id, set()).add(conn.name)

            qty = None
            mv = None
            cb = None
            try:
                if it.get("qty") is not None and str(it.get("qty")).strip() != "":
                    qty = float(it.get("qty"))
            except Exception:
                qty = None
            try:
                if it.get("market_value") is not None and str(it.get("market_value")).strip() != "":
                    mv = float(it.get("market_value"))
            except Exception:
                mv = None
            try:
                if it.get("cost_basis_total") is not None and str(it.get("cost_basis_total")).strip() != "":
                    cb = float(it.get("cost_basis_total"))
            except Exception:
                cb = None

            raw_positions.append(
                HoldingPosition(
                    account_id=acct_id,
                    account_name=acct_name,
                    taxpayer_type=taxpayer_type,
                    symbol=symbol,
                    qty=qty,
                    latest_price=None,
                    latest_price_as_of=None,
                    latest_price_fetched_at=None,
                    market_value=mv,
                    market_value_snapshot=mv,
                    cost_basis_total=cb,
                    pnl_amount=None,
                    pnl_pct=None,
                    tax_status=None,
                    entered_date=None,
                    wash_safe_exit_date=None,
                    as_of=snap.as_of,
                )
            )
        if source_used:
            data_sources.append(
                {
                    "connection_id": conn.id,
                    "connection_name": conn.name,
                    "provider": conn.provider,
                    "broker": conn.broker,
                    "connector": conn.connector,
                    "as_of": snap.as_of,
                }
            )
            if max_as_of is None or (snap.as_of and snap.as_of > max_as_of):
                max_as_of = snap.as_of

    # Manual physical holdings (bullion): included in Holdings view and valued using the user-entered unit price.
    try:
        bh_rows = session.query(BullionHolding).filter(BullionHolding.account_id.in_(included_account_ids)).all()
    except Exception:
        bh_rows = []
    for bh in bh_rows:
        aid = int(getattr(bh, "account_id", 0) or 0)
        if aid not in acct_by_id:
            continue
        acct_name = acct_by_id[aid][0].name
        taxpayer_type = acct_by_id[aid][1].type
        metal = str(getattr(bh, "metal", "") or "").strip().upper()
        if metal not in {"GOLD", "SILVER"}:
            continue
        try:
            qty = float(getattr(bh, "quantity", 0.0) or 0.0)
        except Exception:
            qty = 0.0
        try:
            px = float(getattr(bh, "unit_price", 0.0) or 0.0)
        except Exception:
            px = 0.0
        try:
            mv = float(qty) * float(px)
        except Exception:
            mv = None
        try:
            cost_basis = getattr(bh, "cost_basis_total", None)
            cb_total = float(cost_basis) if cost_basis is not None else None
        except Exception:
            cb_total = None
        try:
            d = getattr(bh, "as_of_date", None)
            as_of_dt = dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc) if d is not None else None
        except Exception:
            as_of_dt = None

        raw_positions.append(
            HoldingPosition(
                account_id=aid,
                account_name=acct_name,
                taxpayer_type=taxpayer_type,
                symbol=f"BULLION:{metal}",
                qty=qty,
                latest_price=px,
                latest_price_as_of=d if d is not None else None,
                latest_price_fetched_at=as_of_dt,
                market_value=mv,
                market_value_snapshot=mv,
                cost_basis_total=cb_total,
                pnl_amount=None,
                pnl_pct=None,
                tax_status=None,
                entered_date=None,
                wash_safe_exit_date=None,
                as_of=as_of_dt,
            )
        )
        if as_of_dt is not None and (max_as_of is None or as_of_dt > max_as_of):
            max_as_of = as_of_dt

    # If multiple connections contribute holdings for the same internal account, warn (likely duplicate feeds).
    for acct_id, sources in seen_sources_by_account.items():
        if len(sources) > 1:
            warnings.append(f"Account '{acct_by_id[acct_id][0].name}' has holdings from multiple connections: {sorted(sources)}. This may double-count.")

    # Apply account filter.
    if account_id is not None:
        raw_positions = [p for p in raw_positions if p.account_id == account_id]

    # Precompute "entered" date per (account, symbol) for displayed accounts (lots first, then BUY txns).
    entered_by_key: dict[tuple[int, str], dt.date] = {}
    # Precompute lot-based cost basis totals per (account, symbol) (preferred over broker snapshot basis).
    lot_basis_by_key: dict[tuple[int, str], float] = {}
    lot_qty_by_key: dict[tuple[int, str], float] = {}
    lot_basis_source_by_key: dict[tuple[int, str], str] = {}  # tax_lot|position_lot
    lot_status_by_key: dict[tuple[int, str], str] = {}  # ST|LT|MIXED
    if included_account_ids:
        # Prefer reconstructed TaxLot lots when available, but fall back to PositionLots for any symbols/accounts
        # that do not have reconstructed lots (or have reconstructed lots missing basis).
        taxlot_rows = (
            session.query(TaxLot, Security)
            .join(Security, Security.id == TaxLot.security_id)
            .filter(
                TaxLot.account_id.in_(included_account_ids),
                TaxLot.source == "RECONSTRUCTED",
                TaxLot.quantity_open > 0,
            )
            .all()
        )
        keys_with_taxlots: set[tuple[int, str]] = set()
        if taxlot_rows:
            per_key_dates_tax: dict[tuple[int, str], list[dt.date]] = {}
            for lot, sec in taxlot_rows:
                t = str(sec.ticker or "").upper()
                if not t:
                    continue
                k = (int(lot.account_id), t)
                keys_with_taxlots.add(k)
                per_key_dates_tax.setdefault(k, []).append(lot.acquired_date)
                if lot.basis_open is not None:
                    lot_basis_by_key[k] = float(lot_basis_by_key.get(k) or 0.0) + float(lot.basis_open)
                    lot_basis_source_by_key[k] = "tax_lot"
            for k, dates in per_key_dates_tax.items():
                entered_by_key[k] = min(dates)
                has_st = any((today_d - d).days < 365 for d in dates)
                has_lt = any((today_d - d).days >= 365 for d in dates)
                lot_status_by_key[k] = "MIXED" if (has_st and has_lt) else ("ST" if has_st else "LT")

        # PositionLots fallback for keys not covered by reconstructed TaxLots (or missing TaxLot basis).
        lot_rows = session.query(PositionLot).filter(PositionLot.account_id.in_(included_account_ids))
        per_key_dates_pos: dict[tuple[int, str], list[dt.date]] = {}
        for lot in lot_rows:
            t = str(lot.ticker or "").upper()
            if not t:
                continue
            k = (int(lot.account_id), t)
            allow = (k not in keys_with_taxlots) or (k in keys_with_taxlots and k not in lot_basis_by_key)
            if not allow:
                continue
            per_key_dates_pos.setdefault(k, []).append(lot.acquisition_date)
            basis = float(lot.adjusted_basis_total) if lot.adjusted_basis_total is not None else float(lot.basis_total)
            lot_basis_by_key[k] = float(lot_basis_by_key.get(k) or 0.0) + basis
            try:
                lot_qty_by_key[k] = float(lot_qty_by_key.get(k) or 0.0) + float(lot.qty)
            except Exception:
                pass
            lot_basis_source_by_key[k] = "position_lot"
        for k, dates in per_key_dates_pos.items():
            if k not in entered_by_key:
                entered_by_key[k] = min(dates)
            if k not in lot_status_by_key:
                has_st = any((today_d - d).days < 365 for d in dates)
                has_lt = any((today_d - d).days >= 365 for d in dates)
                lot_status_by_key[k] = "MIXED" if (has_st and has_lt) else ("ST" if has_st else "LT")

        buy_rows = (
            session.query(Transaction.account_id, Transaction.ticker, func.min(Transaction.date))
            .filter(Transaction.account_id.in_(included_account_ids), Transaction.type == "BUY", Transaction.ticker.is_not(None))
            .group_by(Transaction.account_id, Transaction.ticker)
            .all()
        )
        for aid, t, d in buy_rows:
            if aid and t and d:
                k = (int(aid), str(t).upper())
                if k not in entered_by_key:
                    entered_by_key[k] = d

    # Precompute recent buys (last 30d) across wash scope, then compute wash-safe exit date by symbol.
    # Conservative: based on executed buys only; assumes no future buys in the following 30 days.
    recent_start = today_d - dt.timedelta(days=30)
    recent_buy_dates_by_ticker: dict[str, dt.date] = {}
    if wash_account_ids:
        rows = (
            session.query(Transaction.ticker, func.max(Transaction.date))
            .filter(
                Transaction.account_id.in_(wash_account_ids),
                Transaction.type == "BUY",
                Transaction.ticker.is_not(None),
                Transaction.date >= recent_start,
                Transaction.date <= today_d,
            )
            .group_by(Transaction.ticker)
            .all()
        )
        for t, d in rows:
            if t and d:
                recent_buy_dates_by_ticker[str(t).upper()] = d

    # Substitute group expansion for "substantially identical".
    sec_rows = session.query(Security.ticker, Security.substitute_group_id).all()
    sub_group_by_ticker: dict[str, Optional[int]] = {str(t).upper(): sg for t, sg in sec_rows if t}
    tickers_by_group: dict[int, list[str]] = {}
    for t, sg in sub_group_by_ticker.items():
        if sg is None:
            continue
        tickers_by_group.setdefault(int(sg), []).append(t)

    def _wash_safe_date_for_symbol(symbol: str) -> Optional[dt.date]:
        base = symbol.upper()
        tickers = {base}
        sg = sub_group_by_ticker.get(base)
        if sg is not None:
            for t in tickers_by_group.get(int(sg), []):
                tickers.add(t)
        last_buy: Optional[dt.date] = None
        for t in tickers:
            d = recent_buy_dates_by_ticker.get(t)
            if d and (last_buy is None or d > last_buy):
                last_buy = d
        if last_buy is None:
            return today_d
        # If there was a buy within the last 30 days, selling at a loss today could wash.
        if (today_d - last_buy).days <= 30:
            return last_buy + dt.timedelta(days=31)
        return today_d

    # Aggregate positions.
    # Keep positions separate by (account, symbol) so we can attribute each row to a brokerage account.
    agg: dict[tuple[Optional[int], str], dict[str, Any]] = {}
    cost_missing_symbols: set[tuple[Optional[int], str]] = set()
    for p in raw_positions:
        key = (p.account_id, p.symbol)
        r = agg.get(key)
        acct_type = None
        if p.account_id is not None and p.account_id in acct_by_id:
            acct_type = (acct_by_id[p.account_id][0].account_type or "").upper()
        if r is None:
            agg[key] = {
                "symbol": p.symbol,
                "qty": float(p.qty) if p.qty is not None else None,
                "market_value": float(p.market_value) if p.market_value is not None else None,
                "cost_basis_total": float(p.cost_basis_total) if p.cost_basis_total is not None else None,
                "as_of": p.as_of,
                "account_id": p.account_id,
                "account_name": p.account_name,
                "taxpayer_type": p.taxpayer_type,
                "account_types": {acct_type} if acct_type else set(),
            }
            if p.cost_basis_total is None:
                cost_missing_symbols.add(key)
        else:
            if acct_type:
                r.setdefault("account_types", set()).add(acct_type)
            if p.qty is not None:
                r["qty"] = float(r["qty"] or 0.0) + float(p.qty)
            if p.market_value is not None:
                r["market_value"] = float(r["market_value"] or 0.0) + float(p.market_value)
            if p.cost_basis_total is not None:
                r["cost_basis_total"] = float(r["cost_basis_total"] or 0.0) + float(p.cost_basis_total)
            else:
                cost_missing_symbols.add(key)

    positions: list[HoldingPosition] = []
    # Load latest cached prices (offline) when requested. If no cache exists for a symbol, we fall back to the snapshot
    # valuation and derive a unit price from snapshot MV/qty when possible.
    symbols = sorted({str(agg[k].get("symbol") or "").upper() for k in agg.keys()})
    latest_px_by_symbol = (
        _latest_prices_for_symbols(prices_dir=prices_root, symbols=symbols, as_of=today_d) if prices_root is not None else {}
    )

    basis_mismatch_warned: set[tuple[int, str]] = set()
    for key in sorted(agg.keys(), key=lambda k: (str(agg[k].get("account_name") or ""), str(agg[k].get("symbol") or ""))):
        r = agg[key]
        acct_id = r.get("account_id")
        sym = str(r.get("symbol") or "").upper()
        basis = None
        if acct_id is not None:
            k2 = (int(acct_id), sym)
            basis = lot_basis_by_key.get(k2)
            # Guardrail: PositionLot rows can be created opportunistically from BUY transactions (MVP),
            # and do not model sales/closures. In those cases, summing PositionLot basis can vastly
            # overstate current cost basis. If the summed lot quantity doesn't approximately match the
            # current position quantity, prefer broker snapshot basis instead.
            if (
                basis is not None
                and lot_basis_source_by_key.get(k2) == "position_lot"
                and k2 in lot_qty_by_key
                and r.get("qty") is not None
            ):
                try:
                    pos_qty = float(r.get("qty") or 0.0)
                    lot_qty = float(lot_qty_by_key.get(k2) or 0.0)
                    if abs(pos_qty) > 1e-9:
                        rel = abs(lot_qty - pos_qty) / abs(pos_qty)
                        if rel > 0.25:
                            basis = None
                            if k2 not in basis_mismatch_warned:
                                basis_mismatch_warned.add(k2)
                                warnings.append(
                                    f"Cost basis for {r.get('account_name') or acct_id}:{sym} appears to be from historical BUY lots "
                                    f"(lot qty {lot_qty:g} vs position qty {pos_qty:g}); using broker snapshot basis."
                                )
                except Exception:
                    pass
        # If no lots basis, fall back to broker snapshot basis only when all contributing rows had it.
        if basis is None and key not in cost_missing_symbols:
            basis = r.get("cost_basis_total")
        mv_snapshot = r.get("market_value")
        qty = r.get("qty")

        latest_price = None
        latest_price_as_of = None
        latest_price_fetched_at = None
        px_hit = latest_px_by_symbol.get(sym)
        if px_hit is not None:
            latest_price_as_of, latest_price, latest_price_fetched_at = px_hit
        elif qty is not None and mv_snapshot is not None:
            try:
                q = float(qty)
                if abs(q) > 1e-12:
                    latest_price = float(mv_snapshot) / q
                    latest_price_as_of = (r.get("as_of") or max_as_of).date() if (r.get("as_of") or max_as_of) else None
                    latest_price_fetched_at = (r.get("as_of") or max_as_of)
            except Exception:
                latest_price = None
                latest_price_as_of = None
                latest_price_fetched_at = None

        mv = None
        if qty is not None and latest_price is not None:
            try:
                mv = float(qty) * float(latest_price)
            except Exception:
                mv = None
        if mv is None:
            mv = mv_snapshot

        pnl_amt: Optional[float] = None
        pnl_pct: Optional[float] = None
        if mv is not None and basis is not None:
            pnl_amt = float(mv) - float(basis)
            if abs(float(basis)) > 1e-9:
                pnl_pct = pnl_amt / float(basis)

        tax_status: Optional[str] = None
        # Determine tax status applicability:
        # - Based on the account for that position row.
        acct_type_view = None
        if acct_id is not None and acct_id in acct_by_id:
            acct_type_view = (acct_by_id[acct_id][0].account_type or "").upper()
        ira_only = acct_type_view == "IRA"
        # IRA: tax status not meaningful for wash/tax harvesting.
        if ira_only:
            tax_status = "N/A"
        else:
            if acct_id is not None:
                tax_status = lot_status_by_key.get((int(acct_id), sym))
            if tax_status is None:
                # Fallback: if we know entered date, approximate.
                ed = entered_by_key.get((int(acct_id), sym)) if acct_id is not None else None
                if ed is not None:
                    tax_status = "LT" if (today_d - ed).days >= 365 else "ST"

        wash_exit = _wash_safe_date_for_symbol(sym)
        positions.append(
            HoldingPosition(
                account_id=acct_id,
                account_name=r.get("account_name") or ("Unknown" if acct_id is None else str(acct_id)),
                taxpayer_type=r.get("taxpayer_type"),
                symbol=r["symbol"],
                qty=qty,
                latest_price=latest_price,
                latest_price_as_of=latest_price_as_of,
                latest_price_fetched_at=latest_price_fetched_at,
                market_value=mv,
                market_value_snapshot=mv_snapshot,
                cost_basis_total=basis,
                pnl_amount=pnl_amt,
                pnl_pct=pnl_pct,
                tax_status=tax_status or "—",
                entered_date=entered_by_key.get((int(acct_id), sym)) if acct_id is not None else None,
                wash_safe_exit_date=wash_exit,
                as_of=r.get("as_of"),
            )
        )

    # Cash balances (prefer CashBalance table; fallback to snapshot cash items).
    cash_by_acct = latest_cash_by_account(session)
    cash_total = 0.0
    cash_asof: Optional[dt.date] = None
    if account_id is not None:
        if account_id in cash_by_acct:
            cash_total = float(cash_by_acct[account_id].amount)
            cash_asof = cash_by_acct[account_id].as_of
        else:
            cash_total = float(cash_from_snapshot_by_account.get(account_id) or 0.0)
    else:
        for aid in included_account_ids:
            if aid in cash_by_acct:
                cash_total += float(cash_by_acct[aid].amount)
                d = cash_by_acct[aid].as_of
                cash_asof = d if cash_asof is None or d > cash_asof else cash_asof
            else:
                cash_total += float(cash_from_snapshot_by_account.get(aid) or 0.0)

    total_value = cash_total + sum(float(p.market_value or 0.0) for p in positions)

    account_name = "Combined"
    if account_id is not None and account_id in acct_by_id:
        account_name = acct_by_id[account_id][0].name

    total_market_value = sum(float(p.market_value or 0.0) for p in positions)
    total_initial_cost = sum(float(p.cost_basis_total or 0.0) for p in positions if p.cost_basis_total is not None)
    total_pnl_amount = sum(float(p.pnl_amount or 0.0) for p in positions if p.pnl_amount is not None)
    missing_cost_count = sum(
        1
        for p in positions
        if p.market_value is not None and p.cost_basis_total is None
    )
    # Average P&L%: compute portfolio-level return, excluding cash (and only where cost is known).
    invested_cost = sum(
        float(p.cost_basis_total or 0.0) for p in positions if p.cost_basis_total is not None
    )
    invested_pnl = sum(float(p.pnl_amount or 0.0) for p in positions if p.pnl_amount is not None)
    avg_pnl_pct = (invested_pnl / invested_cost) if invested_cost > 1e-9 else None

    # Calendar-year return (simple, planning-grade): uses earliest snapshot in the year as baseline.
    year_start = dt.date(today_d.year, 1, 1)
    year_start_dt = dt.datetime.combine(year_start, dt.time.min, tzinfo=dt.timezone.utc)
    ytd_start_value: Optional[float] = None
    ytd_start_asof: Optional[dt.datetime] = None
    gain_begin_value: Optional[float] = None
    gain_begin_label = "Begin value (baseline near Jan 1)"
    ytd_contributions = 0.0
    ytd_withdrawals = 0.0
    ytd_dividends_received = 0.0
    ytd_dividends = 0.0
    ytd_interest_net = 0.0
    ytd_interest_received = 0.0
    ytd_interest_paid = 0.0
    ytd_withholding = 0.0
    ytd_fees = 0.0
    ytd_net_cashflow = 0.0
    ytd_gain_value: Optional[float] = None
    ytd_return_pct: Optional[float] = None
    recent_cashflows: list[dict[str, Any]] = []
    ytd_transfers: list[dict[str, Any]] = []
    ytd_deposit_count = 0
    ytd_withdrawal_count = 0
    pnl_planning_value = 0.0
    pnl_return_on_cost: Optional[float] = None

    # Cashflow metrics: always compute from Jan 1 (even if we can't compute a year-start baseline value).
    flow_start_date = year_start
    # When the same brokerage account is imported by multiple connections (e.g., offline + web),
    # dedupe cashflows by (provider_account_id, provider_txn_id) to avoid double-counting in summaries.
    def _provider_flow_key(tx: Transaction, etm: ExternalTransactionMap | None) -> str | None:
        links = tx.lot_links_json or {}
        provider_acct = str(links.get("provider_account_id") or "").strip()
        provider_txn_id = str((getattr(etm, "provider_txn_id", None) if etm is not None else None) or links.get("provider_txn_id") or "").strip()
        if provider_acct and provider_txn_id:
            return f"{provider_acct}|{provider_txn_id}"
        return None

    seen_flow_keys: set[str] = set()
    suppressed_dupe_cashflows = 0

    transfers = (
        session.query(Transaction, ExternalTransactionMap, ExternalConnection)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .filter(
            Transaction.account_id.in_(included_account_ids),
            Transaction.type == "TRANSFER",
            Transaction.date >= flow_start_date,
            Transaction.date <= today_d,
        )
        # Only include external rows from the preferred connections for this view.
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(conn_ids)))
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .all()
    )
    for tx, etm, conn_row in transfers:
        k = _provider_flow_key(tx, etm)
        if k and k in seen_flow_keys:
            suppressed_dupe_cashflows += 1
            continue
        if k:
            seen_flow_keys.add(k)
        try:
            amt = float(tx.amount or 0.0)
        except Exception:
            continue
        if amt >= 0:
            ytd_contributions += amt
            ytd_deposit_count += 1
        else:
            ytd_withdrawals += abs(amt)
            ytd_withdrawal_count += 1
        acct_name = acct_by_id.get(tx.account_id, (None, None))[0].name if tx.account_id in acct_by_id else str(tx.account_id)
        links = tx.lot_links_json or {}
        ytd_transfers.append(
            {
                "date": tx.date,
                "account": acct_name,
                "source": (conn_row.name if conn_row is not None else "Manual"),
                "amount": amt,
                "description": str(links.get("description") or ""),
            }
        )

    div_txns = (
        session.query(Transaction, ExternalTransactionMap, ExternalConnection)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .filter(
            Transaction.account_id.in_(included_account_ids),
            Transaction.type.in_(["DIV", "INT"]),
            Transaction.date >= flow_start_date,
            Transaction.date <= today_d,
        )
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(conn_ids)))
        .all()
    )
    for tx, etm, _conn_row in div_txns:
        k = _provider_flow_key(tx, etm)
        if k and k in seen_flow_keys:
            suppressed_dupe_cashflows += 1
            continue
        if k:
            seen_flow_keys.add(k)
        try:
            amt = float(tx.amount or 0.0)
            if tx.type == "DIV":
                ytd_dividends += amt
            else:
                ytd_interest_net += amt
                if amt >= 0:
                    ytd_interest_received += amt
                else:
                    ytd_interest_paid += abs(amt)
        except Exception:
            continue

    cash_txns = (
        session.query(Transaction, ExternalTransactionMap, ExternalConnection)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .filter(
            Transaction.account_id.in_(included_account_ids),
            Transaction.type.in_(["WITHHOLDING", "FEE"]),
            Transaction.date >= flow_start_date,
            Transaction.date <= today_d,
        )
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(conn_ids)))
        .all()
    )
    for tx, etm, _conn_row in cash_txns:
        k = _provider_flow_key(tx, etm)
        if k and k in seen_flow_keys:
            suppressed_dupe_cashflows += 1
            continue
        if k:
            seen_flow_keys.add(k)
        try:
            amt = float(tx.amount or 0.0)
        except Exception:
            continue
        if tx.type == "WITHHOLDING":
            ytd_withholding += abs(amt)
        elif tx.type == "FEE":
            ytd_fees += abs(amt)

    from src.db.models import IncomeEvent

    incs = (
        session.query(IncomeEvent)
        .filter(
            IncomeEvent.account_id.in_(included_account_ids),
            IncomeEvent.type.in_(["DIVIDEND", "INTEREST"]),
            IncomeEvent.date >= flow_start_date,
            IncomeEvent.date <= today_d,
        )
        .all()
    )
    for ev in incs:
        try:
            amt = float(ev.amount or 0.0)
        except Exception:
            continue
        if ev.type == "DIVIDEND":
            ytd_dividends += amt
        else:
            ytd_interest_net += amt
            if amt >= 0:
                ytd_interest_received += amt
            else:
                ytd_interest_paid += abs(amt)

    inc_withh = (
        session.query(IncomeEvent)
        .filter(
            IncomeEvent.account_id.in_(included_account_ids),
            IncomeEvent.type.in_(["WITHHOLDING", "FEE"]),
            IncomeEvent.date >= flow_start_date,
            IncomeEvent.date <= today_d,
        )
        .all()
    )
    for ev in inc_withh:
        try:
            amt = float(ev.amount or 0.0)
        except Exception:
            continue
        if ev.type == "WITHHOLDING":
            ytd_withholding += abs(amt)
        elif ev.type == "FEE":
            ytd_fees += abs(amt)

    ytd_dividends_received = float(ytd_dividends)
    ytd_net_cashflow = (
        float(ytd_contributions)
        - float(ytd_withdrawals)
        + float(ytd_dividends_received)
        + float(ytd_interest_net)
        - float(ytd_withholding)
        - float(ytd_fees)
    )

    recent = (
        session.query(Transaction, ExternalTransactionMap, ExternalConnection)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .filter(
            Transaction.account_id.in_(included_account_ids),
            Transaction.type.notin_(["BUY", "SELL"]),
            Transaction.date >= flow_start_date,
            Transaction.date <= today_d,
        )
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(conn_ids)))
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .all()
    )
    seen_recent_keys: set[str] = set()
    for tx, etm, conn_row in recent:
        k = _provider_flow_key(tx, etm)
        if k and k in seen_recent_keys:
            continue
        if k:
            seen_recent_keys.add(k)
        acct_name = acct_by_id.get(tx.account_id, (None, None))[0].name if tx.account_id in acct_by_id else str(tx.account_id)
        links = tx.lot_links_json or {}
        recent_cashflows.append(
            {
                "date": tx.date,
                "account": acct_name,
                "source": (conn_row.name if conn_row is not None else "Manual"),
                "type": tx.type,
                "symbol": tx.ticker,
                "amount": float(tx.amount or 0.0),
                "description": str(links.get("description") or ""),
            }
        )

    if suppressed_dupe_cashflows:
        warnings.append(
            f"Suppressed {suppressed_dupe_cashflows} duplicate cashflow row(s) (deduped by provider account + provider transaction id)."
        )

    # Year-start baseline for "calendar-year return": best-effort.
    # Preferred: holdings snapshot near Jan 1 (within ~14 days) for each connection in-scope.
    # Fallback: estimate begin value from positions cost basis + cash (planning-grade).
    baseline_issue: Optional[str] = None
    if conn_ids:
        baseline_snaps: list[ExternalHoldingSnapshot] = []
        for cid in conn_ids:
            snap = (
                session.query(ExternalHoldingSnapshot)
                .filter(ExternalHoldingSnapshot.connection_id == cid, ExternalHoldingSnapshot.as_of >= year_start_dt)
                .order_by(ExternalHoldingSnapshot.as_of.asc())
                .first()
            )
            if snap is None:
                baseline_issue = "no holdings snapshot available after Jan 1 for one or more connections"
                baseline_snaps = []
                break
            baseline_snaps.append(snap)

        if baseline_snaps:
            min_asof = min(s.as_of for s in baseline_snaps if s.as_of is not None)
            max_asof = max(s.as_of for s in baseline_snaps if s.as_of is not None)
            if min_asof and max_asof and (max_asof - min_asof) > dt.timedelta(days=3):
                baseline_issue = "baseline holdings snapshots across connections are not aligned in time (>3 days apart)"
            else:
                # Require baseline to be close to Jan 1, otherwise we can only provide an estimate.
                if max_asof and (max_asof.date() - year_start).days > 14:
                    baseline_issue = "no holdings snapshot near Jan 1 (need one within ~14 days)"
                else:
                    baseline_positions = 0.0
                    baseline_cash = 0.0
                    for snap in baseline_snaps:
                        payload = snap.payload_json or {}
                        items = payload.get("items") or []
                        if not isinstance(items, list):
                            continue
                        for it in items:
                            if not isinstance(it, dict):
                                continue
                            provider_acct = str(it.get("provider_account_id") or "").strip()
                            symbol = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
                            if not symbol:
                                continue
                            acct_id = map_by_conn_provider.get((snap.connection_id, provider_acct))
                            if acct_id is None or acct_id not in included_account_ids:
                                continue
                            try:
                                mv = float(it.get("market_value") or 0.0)
                            except Exception:
                                mv = 0.0
                            if symbol.startswith("CASH:"):
                                baseline_cash += mv
                            else:
                                baseline_positions += mv
                    ytd_start_asof = max_asof

                    # Prefer CashBalance table for baseline cash if available (USD), else fallback to snapshot cash.
                    baseline_date = ytd_start_asof.date() if ytd_start_asof else year_start
                    baseline_cash_by_acct: dict[int, float] = {}
                    # For each included account, take the latest cash balance on/before baseline_date.
                    for aid in included_account_ids:
                        cb = (
                            session.query(CashBalance)
                            .filter(CashBalance.account_id == aid, CashBalance.as_of_date <= baseline_date)
                            .order_by(CashBalance.as_of_date.desc(), CashBalance.id.desc())
                            .first()
                        )
                        if cb is not None:
                            baseline_cash_by_acct[aid] = float(cb.amount or 0.0)
                    if baseline_cash_by_acct:
                        baseline_cash = sum(baseline_cash_by_acct.values())
                    ytd_start_value = baseline_positions + baseline_cash

    # Gain/return:
    # - Preferred: use holdings snapshot baseline near Jan 1
    # - Fallback: use cost-basis + cash as a begin value estimate (planning-grade)
    if ytd_start_value is not None:
        gain_begin_value = float(ytd_start_value)
        gain_begin_label = "Begin value (baseline near Jan 1)"
    else:
        est_cost_plus_cash = float(total_initial_cost or 0.0) + float(cash_total or 0.0)
        if est_cost_plus_cash > 1e-9:
            gain_begin_value = est_cost_plus_cash
            gain_begin_label = "Begin value (estimate: positions cost + cash; no Jan snapshot)"
        else:
            # Last-resort: if we can't estimate a begin value from basis, fall back to current market value.
            # This is not a true calendar-year return; it is provided to avoid blank metrics.
            est_market = float(total_value or 0.0)
            if est_market > 1e-9:
                gain_begin_value = est_market
                gain_begin_label = "Begin value (estimate: current market value; missing Jan snapshot and basis)"
                reason = f": {baseline_issue}" if baseline_issue else ""
                warnings.append(f"Calendar-year return is estimated using current market value as the begin value{reason}.")

    if gain_begin_value is not None and gain_begin_value > 1e-9:
        # Gain Value formula (extended to include interest): (End − Begin) + Withdrawals + Dividends + Interest − Contributions
        ytd_gain_value = (
            (float(total_value) - float(gain_begin_value))
            + float(ytd_withdrawals)
            + float(ytd_dividends_received)
            + float(ytd_interest_net)
            - float(ytd_contributions)
        )
        ytd_return_pct = float(ytd_gain_value) / float(gain_begin_value)
    elif gain_begin_value is not None and gain_begin_value <= 1e-9:
        warnings.append("Calendar-year return not computed: begin value is zero.")

    # Always-available planning P&L (not calendar-year): positions unrealized P&L + income - fees - withholding.
    pnl_planning_value = float(total_pnl_amount or 0.0) + float(ytd_dividends_received) + float(ytd_interest_net) - float(ytd_fees) - float(ytd_withholding)
    if total_initial_cost is not None and float(total_initial_cost) > 1e-9:
        pnl_return_on_cost = float(pnl_planning_value) / float(total_initial_cost)

    return HoldingsView(
        scope=sc,
        scope_label=_scope_label(sc),
        account_id=account_id,
        account_name=account_name,
        as_of=max_as_of,
        positions=positions,
        total_value=total_value,
        total_market_value=total_market_value,
        total_initial_cost=total_initial_cost,
        total_pnl_amount=total_pnl_amount,
        avg_pnl_pct=avg_pnl_pct,
        totals_missing_cost_count=missing_cost_count,
        cash_total=cash_total,
        cash_asof=cash_asof,
        ytd_start_value=ytd_start_value,
        ytd_start_asof=ytd_start_asof,
        gain_begin_value=gain_begin_value,
        gain_begin_label=gain_begin_label,
        ytd_contributions=ytd_contributions,
        ytd_withdrawals=ytd_withdrawals,
        ytd_dividends_received=ytd_dividends_received,
        ytd_dividends=ytd_dividends,
        ytd_interest_net=ytd_interest_net,
        ytd_interest_received=ytd_interest_received,
        ytd_interest_paid=ytd_interest_paid,
        ytd_withholding=ytd_withholding,
        ytd_fees=ytd_fees,
        ytd_net_cashflow=ytd_net_cashflow,
        ytd_gain_value=ytd_gain_value,
        ytd_return_pct=ytd_return_pct,
        recent_cashflows=recent_cashflows,
        ytd_transfers=ytd_transfers,
        ytd_deposit_count=ytd_deposit_count,
        ytd_withdrawal_count=ytd_withdrawal_count,
        pnl_planning_value=pnl_planning_value,
        pnl_return_on_cost=pnl_return_on_cost,
        warnings=warnings,
        data_sources=sorted(data_sources, key=lambda r: str(r.get("connection_name") or "")),
    )
