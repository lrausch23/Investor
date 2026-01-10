from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.core.sync_runner import run_sync
from src.db.models import ExternalConnection, TaxpayerEntity, Transaction


def _make_connection(session, *, data_dir: Path) -> ExternalConnection:
    tp = TaxpayerEntity(name="Trust", type="TRUST", tax_id_last4="1234", notes=None)
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="RJ (Offline)",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.flush()
    return conn


def test_rj_qfx_sync_is_idempotent(session, tmp_path: Path):
    data_dir = tmp_path / "rj"
    data_dir.mkdir(parents=True, exist_ok=True)
    src = Path("tests/fixtures/rj_qfx_minimal.qfx").read_text(encoding="utf-8-sig", errors="ignore")
    (data_dir / "rj1.qfx").write_text(src, encoding="utf-8")

    conn = _make_connection(session, data_dir=data_dir)

    # First run imports two transactions (BUY + DIV) and one holdings snapshot.
    run_sync(
        session,
        connection_id=conn.id,
        mode="INCREMENTAL",
        start_date=dt.date(2025, 1, 1),
        end_date=None,
        actor="test",
    )
    tx_count_1 = session.query(Transaction).count()
    assert tx_count_1 == 2

    # Second run (incremental, same file hash) should not import duplicates.
    run_sync(
        session,
        connection_id=conn.id,
        mode="INCREMENTAL",
        start_date=dt.date(2025, 1, 1),
        end_date=None,
        actor="test",
    )
    tx_count_2 = session.query(Transaction).count()
    assert tx_count_2 == tx_count_1


def test_rj_qfx_overlapping_files_dedupe_by_fitid(session, tmp_path: Path):
    data_dir = tmp_path / "rj"
    data_dir.mkdir(parents=True, exist_ok=True)
    base = Path("tests/fixtures/rj_qfx_minimal.qfx").read_text(encoding="utf-8-sig", errors="ignore")
    # First file contains FITID F1/F2.
    (data_dir / "rj1.qfx").write_text(base, encoding="utf-8")
    # Second file overlaps F1/F2 but also adds a new FITID F3.
    extra = base.replace(
        "</INVTRANLIST>",
        """
          <INCOME>
            <INVTRAN>
              <FITID>F3
              <DTTRADE>20250120
              <MEMO>Dividend 2
            </INVTRAN>
            <SECID>
              <UNIQUEID>037833100
              <UNIQUEIDTYPE>CUSIP
            </SECID>
            <TOTAL>6.00
            <INCOMETYPE>DIV
          </INCOME>
        </INVTRANLIST>
""",
    )
    (data_dir / "rj2.qfx").write_text(extra, encoding="utf-8")

    conn = _make_connection(session, data_dir=data_dir)

    run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    # Should contain BUY (F1) + DIV (F2) + DIV (F3) = 3 transactions.
    assert session.query(Transaction).count() == 3
