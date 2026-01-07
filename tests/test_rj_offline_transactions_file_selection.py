from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.core.sync_runner import run_sync
from src.db.models import ExternalConnection, TaxpayerEntity, Transaction


def test_rj_offline_selects_transactions_by_content_even_if_filename_looks_holdings(session, tmp_path: Path) -> None:
    work = tmp_path / "rj"
    work.mkdir(parents=True, exist_ok=True)

    # Filename contains "portfolio" which previously caused the offline selector to classify it as HOLDINGS.
    p = work / "portfolio_activity_2026-01-06.csv"
    p.write_text(
        "\n".join(
            [
                "Account,Date,Category,Type,Symbol/CUSIP,Description,Quantity,Price,Amount,Additional Detail",
                "Kolozsi LLC xxxxW554,01/06/2026,Withdrawal,Withdrawal,,Cash,0.00000,$0.00,($20,000.00),*WIRE TO Laszlo Rausch",
                "Kolozsi LLC xxxxW554,12/31/2025,Income,Interest at RJ Bank Deposit Program,,Raymond James Bank Deposit Program,0.00000,$0.00,$5.20,",
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

    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 12, 1),
        end_date=dt.date(2026, 1, 7),
        actor="test",
        pull_holdings=False,
    )
    assert run.status == "SUCCESS"
    assert int(run.coverage_json.get("file_selected") or 0) == 1
    assert int(run.coverage_json.get("file_count") or 0) == 1
    assert session.query(Transaction).count() >= 1

