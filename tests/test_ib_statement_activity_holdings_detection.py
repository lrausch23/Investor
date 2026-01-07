from __future__ import annotations


def test_ib_activity_statement_is_selected_as_holdings_file(session, tmp_path):
    from src.core.sync_runner import _select_offline_holdings_files
    from src.db.models import ExternalConnection, TaxpayerEntity

    tp = TaxpayerEntity(name="Kolozsi LLC", type="TRUST")
    session.add(tp)
    session.flush()

    conn = ExternalConnection(
        name="IB Flex (Web)",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=int(tp.id),
        metadata_json={"data_dir": str(tmp_path)},
    )
    session.add(conn)
    session.flush()

    text = "\n".join(
        [
            "Statement,Data,Title,Activity Statement",
            "Statement,Data,Period,\"January 1, 2025 - January 31, 2025\"",
            "Account Information,Data,Account,U5891158",
            "Net Asset Value,Header,Asset Class,Prior Total,Current Long,Current Short,Current Total,Change",
            "Net Asset Value,Data,Total,0,0,0,450392.42,0",
        ]
    )
    p = tmp_path / "U5891158_202501_202501.csv"
    p.write_text(text, encoding="utf-8")

    selected, metrics = _select_offline_holdings_files(session, connection=conn, mode="FULL", reprocess_files=False)

    assert metrics["holdings_file_total"] == 1
    assert metrics["holdings_file_selected"] == 1
    assert len(selected) == 1
    assert selected[0]["kind"] == "HOLDINGS"
    assert selected[0]["file_name"] == "U5891158_202501_202501.csv"

