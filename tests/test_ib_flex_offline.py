from __future__ import annotations

import datetime as dt
import shutil
from pathlib import Path

from src.core.sync_runner import run_sync
from src.db.models import CashBalance, ExternalConnection, ExternalFileIngest, ExternalTransactionMap, TaxpayerEntity, Transaction


def _mk_conn(session, *, data_dir: str) -> ExternalConnection:
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Offline",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"data_dir": data_dir},
    )
    session.add(conn)
    session.commit()
    return conn


def test_full_sync_imports_transactions_and_sets_earliest(session, tmp_path: Path):
    src_dir = Path("fixtures/ib_flex_offline")
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    shutil.copy(src_dir / "transactions_2025_1.csv", work / "transactions_2025_1.csv")
    shutil.copy(src_dir / "positions_with_cash.csv", work / "positions_2025_12_20.csv")

    conn = _mk_conn(session, data_dir=str(work))
    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2015, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert run.status == "SUCCESS"
    assert run.coverage_json["new_inserted"] == 3
    assert run.coverage_json["txn_count"] == 3
    assert run.coverage_json.get("file_count") == 1
    assert run.coverage_json.get("holdings_items_imported") == 3
    assert run.coverage_json.get("txn_type_counts", {}).get("BUY") == 1
    assert run.coverage_json.get("cash_balances_imported") == 1

    session.refresh(conn)
    assert conn.txn_earliest_available == dt.date(2025, 1, 2)
    assert conn.last_full_sync_at is not None

    assert session.query(Transaction).count() == 3
    assert session.query(CashBalance).count() == 1
    cb = session.query(CashBalance).first()
    assert cb is not None
    assert float(cb.amount) == 1000.0
    types = {t.type for t in session.query(Transaction).all()}
    assert "BUY" in types
    assert types != {"OTHER"}
    assert session.query(ExternalFileIngest).count() == 1


def test_positions_cash_section_imports_cash_balance_latest_report_date(session, tmp_path: Path):
    src_dir = Path("fixtures/ib_flex_offline")
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    shutil.copy(src_dir / "transactions_2025_1.csv", work / "transactions_2025_1.csv")
    shutil.copy(src_dir / "positions_ib_realistic_with_cash.csv", work / "positions_2025-12-20.csv")

    conn = _mk_conn(session, data_dir=str(work))
    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2015, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert run.status == "SUCCESS"
    assert run.coverage_json.get("cash_balances_imported") == 1

    cb = session.query(CashBalance).order_by(CashBalance.as_of_date.desc(), CashBalance.id.desc()).first()
    assert cb is not None
    assert cb.as_of_date == dt.date(2025, 12, 19)
    assert float(cb.amount) == 250.0


def test_incremental_sync_only_processes_new_files(session, tmp_path: Path):
    src_dir = Path("fixtures/ib_flex_offline")
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    shutil.copy(src_dir / "transactions_2025_1.csv", work / "transactions_2025_1.csv")

    conn = _mk_conn(session, data_dir=str(work))
    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2015, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert r1.status == "SUCCESS"
    assert r1.coverage_json["new_inserted"] == 3
    assert session.query(ExternalFileIngest).count() == 1

    shutil.copy(src_dir / "transactions_2025_2.csv", work / "transactions_2025_2.csv")
    r2 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", overlap_days=0, actor="test")
    assert r2.status == "SUCCESS"
    assert r2.coverage_json["new_inserted"] == 2
    assert r2.coverage_json.get("file_count") == 1
    assert session.query(Transaction).count() == 5
    assert session.query(ExternalFileIngest).count() == 2

    # Another incremental run with no new files should ingest nothing new.
    r3 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", overlap_days=0, actor="test")
    assert r3.status == "SUCCESS"
    assert r3.coverage_json["new_inserted"] == 0
    assert r3.coverage_json.get("file_selected") == 0


def test_activity_export_with_preamble_parses(session, tmp_path: Path):
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    p = work / "IB_Activity_Full.csv"
    p.write_text(
        "\n".join(
            [
                '"ClientAccountID"',
                '"U12345"',
                '"ClientAccountID","Symbol","Description","DateTime","TransactionType","Buy/Sell","Quantity","NetCash","TransactionID"',
                '"U12345","AAPL","APPLE INC","20251027;134139","ExchTrade","BUY","10","-1500.25","TXN-1"',
            ]
        ),
        encoding="utf-8",
    )
    conn = _mk_conn(session, data_dir=str(work))
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2015, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert run.status == "SUCCESS"
    assert run.coverage_json["new_inserted"] == 1
    assert run.coverage_json["txn_count"] == 1
    t = session.query(Transaction).order_by(Transaction.id.desc()).first()
    assert t is not None
    assert t.type == "BUY"


def test_activity_classifier_infers_sell_from_qty_and_cash(session, tmp_path: Path):
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    p = work / "activity.csv"
    p.write_text(
        "\n".join(
            [
                '"ClientAccountID","Symbol","DateTime","TransactionType","Quantity","NetCash","TransactionID"',
                '"U12345","AAPL","20251027;134139","ExchTrade","-10","1500.25","TXN-1"',
            ]
        ),
        encoding="utf-8",
    )
    conn = _mk_conn(session, data_dir=str(work))
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2015, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert run.status == "SUCCESS"
    t = session.query(Transaction).order_by(Transaction.id.desc()).first()
    assert t is not None
    assert t.type == "SELL"
    assert float(t.qty) == 10.0
    assert float(t.amount) == 1500.25


def test_positions_schema_variant_parses(session, tmp_path: Path):
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    # Include a small preamble, then a header row with common IB-style column names.
    p = work / "positions_2025-12-20.csv"
    p.write_text(
        "\n".join(
            [
                '"ClientAccountID"',
                '"U12345"',
                '"ClientAccountID","Symbol","Position","MarkPrice","MarketValue"',
                '"U12345","VTI","2","250","500"',
            ]
        ),
        encoding="utf-8",
    )
    # Minimal txn file so accounts map exists.
    (work / "t.csv").write_text("account,date,type,symbol,qty,amount,description,provider_transaction_id\nU12345,2025-01-01,BUY,VTI,1,-250,buy,T1\n", encoding="utf-8")
    conn = _mk_conn(session, data_dir=str(work))
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2015, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert run.status == "SUCCESS"
    assert run.coverage_json.get("holdings_items_imported") == 1


def test_reprocess_files_updates_existing_transaction_classification(session, tmp_path: Path):
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    p = work / "IB_Activity_Full.csv"
    p.write_text(
        "\n".join(
            [
                '"ClientAccountID"',
                '"U12345"',
                '"ClientAccountID","Symbol","Description","DateTime","TransactionType","Buy/Sell","Quantity","NetCash","TransactionID"',
                '"U12345","AAPL","APPLE INC","20251027;134139","ExchTrade","BUY","10","-1500.25","TXN-1"',
            ]
        ),
        encoding="utf-8",
    )
    conn = _mk_conn(session, data_dir=str(work))

    # Simulate a previous import that stored OTHER for the same provider_txn_id.
    # Ensure an account exists for the connection by running once to create account mapping and map.
    r0 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2015, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert r0.status == "SUCCESS"
    # Force transaction type to OTHER to mimic old behavior.
    existing = session.query(Transaction).filter(Transaction.type == "BUY").first()
    assert existing is not None
    existing.type = "OTHER"
    session.flush()

    # Reprocess: should update existing tx back to BUY.
    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2015, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
        reprocess_files=True,
    )
    assert r1.status == "SUCCESS"
    session.refresh(existing)
    assert existing.type == "BUY"
    assert (r1.coverage_json.get("updated_existing") or 0) >= 1


def test_cashflows_import_div_int_withholding_fee_and_transfers(session, tmp_path: Path):
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    p = work / "cashflows.csv"
    p.write_text(
        "\n".join(
            [
                '"ClientAccountID","Date","TransactionType","Description","Symbol","Amount","Currency","Balance","TransactionID"',
                '"U12345","2025-02-01","Deposit","Contribution","", "1000.00","USD","1000.00","CF-1"',
                '"U12345","2025-02-10","Dividend","CASH DIVIDEND","AAPL","25.00","USD","1025.00","CF-2"',
                '"U12345","2025-02-11","Interest","INTEREST","", "5.00","USD","1030.00","CF-3"',
                '"U12345","2025-02-11","Withholding Tax","WITHHOLDING TAX","AAPL","(3.00)","USD","1027.00","CF-4"',
                '"U12345","2025-02-12","Fee","DATA FEE","", "-2.00","USD","1025.00","CF-5"',
                '"U12345","2025-02-15","Withdrawal","Distribution","", "-100.00","USD","925.00","CF-6"',
            ]
        ),
        encoding="utf-8",
    )
    conn = _mk_conn(session, data_dir=str(work))
    r0 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert r0.status == "SUCCESS"

    # Transactions imported with correct types/signs.
    txs = session.query(Transaction).order_by(Transaction.id.asc()).all()
    types = [t.type for t in txs]
    assert "TRANSFER" in types
    assert "DIV" in types
    assert "INT" in types
    assert "WITHHOLDING" in types
    assert "FEE" in types

    dep = next(t for t in txs if (t.lot_links_json or {}).get("provider_txn_id") == "CF-1")
    wdr = next(t for t in txs if (t.lot_links_json or {}).get("provider_txn_id") == "CF-6")
    div = next(t for t in txs if (t.lot_links_json or {}).get("provider_txn_id") == "CF-2")
    intr = next(t for t in txs if (t.lot_links_json or {}).get("provider_txn_id") == "CF-3")
    withh = next(t for t in txs if (t.lot_links_json or {}).get("provider_txn_id") == "CF-4")
    fee = next(t for t in txs if (t.lot_links_json or {}).get("provider_txn_id") == "CF-5")
    assert float(dep.amount) == 1000.0
    assert float(wdr.amount) == -100.0
    assert float(div.amount) == 25.0
    assert float(intr.amount) == 5.0
    assert float(withh.amount) == 3.0  # stored as positive credit
    assert float(fee.amount) == -2.0

    # Cash balance record imported from Balance column.
    assert session.query(CashBalance).count() == 1
    cb = session.query(CashBalance).first()
    assert cb is not None
    assert cb.as_of_date == dt.date(2025, 2, 15)
    assert float(cb.amount) == 925.0

    # Add baseline/end holdings snapshots so the holdings view computes cashflow metrics.
    from src.db.models import ExternalAccountMap, ExternalHoldingSnapshot
    from src.core.external_holdings import build_holdings_view

    m = session.query(ExternalAccountMap).filter(ExternalAccountMap.connection_id == conn.id).first()
    assert m is not None
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2025, 1, 2, 15, 0, 0, tzinfo=dt.timezone.utc),
                payload_json={"items": [{"provider_account_id": m.provider_account_id, "symbol": "AAPL", "qty": 1, "market_value": 1000}]},
            ),
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
                payload_json={"items": [{"provider_account_id": m.provider_account_id, "symbol": "AAPL", "qty": 1, "market_value": 1100}]},
            ),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=None, today=dt.date(2025, 12, 21))
    assert view.ytd_contributions == 1000.0
    assert view.ytd_withdrawals == 100.0
    assert view.ytd_dividends_received == 25.0
    assert view.ytd_interest_net == 5.0
    assert view.ytd_withholding == 3.0
    assert view.ytd_fees == 2.0

    # Dashboard tax summary should pick up dividends/interest/withholding from imported cashflows.
    from src.core.tax_engine import TaxAssumptions, tax_summary_ytd_with_net

    summary = tax_summary_ytd_with_net(session=session, as_of=dt.date(2025, 12, 21), scope="trust", assumptions=TaxAssumptions())
    assert len(summary.rows) == 1
    row = summary.rows[0]
    assert row.income == 30.0  # 25 div + 5 int
    assert row.withholding == 3.0

    from src.core.dashboard_service import build_dashboard

    dash = build_dashboard(session, scope="trust", as_of=dt.date(2025, 12, 21))
    assert dash.cashflows
    cf = dash.cashflows[0]
    assert float(cf.deposits) == 1000.0
    assert float(cf.withdrawals) == 100.0
    assert float(cf.dividends) == 25.0
    assert float(cf.interest) == 5.0
    assert float(cf.withholding) == 3.0
    assert float(cf.fees) == 2.0

    # Reprocessing same file should not double-count.
    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
        reprocess_files=True,
    )
    assert r1.status in {"SUCCESS", "PARTIAL"}
    assert session.query(Transaction).count() == len(txs)


def test_cashflow_export_skips_summary_rows_to_avoid_double_count(session, tmp_path: Path):
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    p = work / "ib_cashflows_detail_summary.csv"
    p.write_text(
        "\n".join(
            [
                "ClientAccountID,Date/Time,Symbol,Description,Amount,Type,TransactionID,LevelOfDetail,CurrencyPrimary",
                # Summary row should be skipped.
                "-,20250402,NVDA,NVDA CASH DIVIDEND (Ordinary Dividend),9,Dividends,,SUMMARY,USD",
                # Detail row should be imported.
                "U12345,20250402;202000,NVDA,NVDA CASH DIVIDEND (Ordinary Dividend),9,Dividends,TX-1,DETAIL,USD",
                # Summary withholding should be skipped.
                "-,20250402,NVDA,NVDA CASH DIVIDEND - US TAX,-2.7,Withholding Tax,,SUMMARY,USD",
                # Detail withholding should be imported (and normalized to positive).
                "U12345,20250402;202000,NVDA,NVDA CASH DIVIDEND - US TAX,-2.7,Withholding Tax,TX-2,DETAIL,USD",
            ]
        ),
        encoding="utf-8",
    )
    conn = _mk_conn(session, data_dir=str(work))
    r0 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert r0.status == "SUCCESS"
    txs = session.query(Transaction).order_by(Transaction.id.asc()).all()
    assert len(txs) == 2
    div = next(t for t in txs if t.type == "DIV")
    wht = next(t for t in txs if t.type == "WITHHOLDING")
    assert float(div.amount) == 9.0
    assert float(wht.amount) == 2.7
