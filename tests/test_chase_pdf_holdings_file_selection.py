from __future__ import annotations


def test_chase_offline_selects_pdf_as_holdings_file(session, tmp_path):
    from src.core.sync_runner import _select_offline_holdings_files
    from src.db.models import ExternalConnection, TaxpayerEntity

    tp = TaxpayerEntity(name="Laszlo Rausch", type="PERSONAL")
    session.add(tp)
    session.flush()

    conn = ExternalConnection(
        name="Chase IRA (Offline)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=int(tp.id),
        metadata_json={"data_dir": str(tmp_path)},
    )
    session.add(conn)
    session.flush()

    (tmp_path / "statement_202501.pdf").write_bytes(b"%PDF-1.4 fake")

    selected, metrics = _select_offline_holdings_files(session, connection=conn, mode="FULL", reprocess_files=False)
    assert metrics["holdings_file_total"] == 1
    assert metrics["holdings_file_selected"] == 1
    assert len(selected) == 1
    assert selected[0]["kind"] == "HOLDINGS"
    assert selected[0]["file_name"] == "statement_202501.pdf"

