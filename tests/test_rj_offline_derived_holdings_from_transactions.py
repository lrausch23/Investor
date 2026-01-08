from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.core.sync_runner import run_sync
from src.db.models import ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity


def test_rj_offline_incremental_rolls_holdings_forward_from_transactions(session, tmp_path: Path) -> None:
    work = tmp_path / "rj"
    work.mkdir(parents=True, exist_ok=True)

    today = dt.date.today()
    base_day = today - dt.timedelta(days=10)
    trade_day = today - dt.timedelta(days=1)

    # Baseline holdings (positions) snapshot file.
    holdings_file = work / f"positions_{base_day.isoformat()}.csv"
    holdings_file.write_text(
        "\n".join(
            [
                "Symbol,Description,Quantity,Current Value,Product Type",
                "APP,APPLOVIN CORPORATION COM CLASS A,125,84193.75,Equity",
                # Cash-like row (no symbol) -> becomes CASH:USD in snapshot.
                ",Raymond James Bank Deposit Program,0,187869.68,Cash & Cash Alternatives",
            ]
        ),
        encoding="utf-8",
    )

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="RJ Offline",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(work)},
    )
    session.add(conn)
    session.commit()

    # First run: import holdings file only (no txns).
    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=base_day - dt.timedelta(days=1),
        end_date=base_day,
        actor="test",
    )
    assert r1.status == "SUCCESS"

    # Add a transactions activity file (sale + purchase + withdrawal).
    tx_file = work / f"portfolio_activity_{trade_day.isoformat()}.csv"
    tx_file.write_text(
        "\n".join(
            [
                "Account,Date,Category,Type,Symbol/CUSIP,Description,Quantity,Price,Amount,Additional Detail",
                f"Kolozsi LLC xxxxW554,{trade_day.strftime('%m/%d/%Y')},Sale/Redemption,Sale,APP,APPLOVIN CORPORATION COM CLASS A,-125.00000,606.57,75821.25,",
                f"Kolozsi LLC xxxxW554,{trade_day.strftime('%m/%d/%Y')},Purchase,Purchase,SGOV,ISHARES TR 0-3 MNTH TREASRY,2000.00000,100.43,(200860.00),",
                f"Kolozsi LLC xxxxW554,{trade_day.strftime('%m/%d/%Y')},Withdrawal,Withdrawal,,Cash,0.00000,0.00,(20000.00),*WIRE TO Someone",
            ]
        ),
        encoding="utf-8",
    )

    r2 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", overlap_days=0, actor="test")
    assert r2.status == "SUCCESS"

    snap = (
        session.query(ExternalHoldingSnapshot)
        .filter(ExternalHoldingSnapshot.connection_id == conn.id)
        .order_by(ExternalHoldingSnapshot.as_of.desc(), ExternalHoldingSnapshot.id.desc())
        .first()
    )
    assert snap is not None
    payload = snap.payload_json or {}
    items = payload.get("items") or []
    symbols = {str(it.get("symbol") or "").strip().upper() for it in items if isinstance(it, dict)}
    assert "SGOV" in symbols
    assert "APP" not in symbols  # sold out


def test_rj_derived_holdings_dedupes_duplicate_sale_cashflows() -> None:
    """
    Regression: when the same SELL appears twice (e.g., once as OTHER, once as SELL due to classifier changes),
    derived cash should not double-count proceeds.
    """
    from src.core.sync_runner import _derive_holdings_snapshot_from_transactions
    from src.db.models import Transaction

    base_holdings = {
        "as_of": "2025-12-31T00:00:00+00:00",
        "items": [
            {"provider_account_id": "RJ:TAXABLE", "symbol": "CASH:USD", "qty": 187869.68, "market_value": 187869.68},
            {"provider_account_id": "RJ:TAXABLE", "symbol": "APP", "qty": 125.0, "market_value": 84193.75},
        ],
        "source_file": "positions_2025-12-31.csv",
    }
    day = dt.date(2026, 1, 6)
    txns = [
        Transaction(
            account_id=1,
            date=day,
            type="OTHER",
            ticker="APP",
            qty=-125.0,
            amount=75821.25,
            lot_links_json={"description": "APPLOVIN CORPORATION COM CLASS A"},
        ),
        Transaction(
            account_id=1,
            date=day,
            type="SELL",
            ticker="APP",
            qty=125.0,
            amount=75821.25,
            lot_links_json={"description": "APPLOVIN CORPORATION COM CLASS A"},
        ),
        Transaction(
            account_id=1,
            date=day,
            type="BUY",
            ticker="SGOV",
            qty=2000.0,
            amount=-200860.00,
            lot_links_json={"description": "ISHARES TR 0-3 MNTH TREASRY"},
        ),
        Transaction(
            account_id=1,
            date=day,
            type="TRANSFER",
            ticker="UNKNOWN",
            qty=None,
            amount=-20000.00,
            lot_links_json={"description": "WIRE OUT"},
        ),
    ]
    derived = _derive_holdings_snapshot_from_transactions(
        base_holdings=base_holdings,
        txns=txns,
        as_of=dt.datetime(2026, 1, 7, 0, 0, tzinfo=dt.timezone.utc),
        source_label="DERIVED_FROM_TXNS:positions_2025-12-31.csv",
    )
    items = derived.get("items") or []
    cash = next(it for it in items if isinstance(it, dict) and str(it.get("symbol") or "").upper().startswith("CASH:"))
    # Cash = base + sale - buy - transfer (sale counted once).
    assert round(float(cash.get("market_value") or 0.0), 2) == round(187869.68 + 75821.25 - 200860.00 - 20000.00, 2)
