from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.core.analytics import wash_risk_summary
from src.core.external_holdings import build_holdings_view
from src.core.sync_runner import run_sync
from src.db.models import Account, CashBalance, ExternalConnection, TaxpayerEntity, Transaction


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_chase_offline_full_import_holdings_cash_and_idempotency(session, tmp_path: Path):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()

    data_dir = tmp_path / "chase"
    data_dir.mkdir(parents=True, exist_ok=True)

    _write(
        data_dir / "chase_activity.csv",
        "\n".join(
            [
                "Date,Type,Symbol,Quantity,Amount,Description",
                "2025-01-10,BUY,VTI,10,($2500.00),Buy VTI",
                "2025-02-10,SELL,VTI,-5,$1300.00,Sell VTI",
                "2025-03-01,Dividends,VTI,, $5.00 ,VTI dividend",
                "2025-03-02,Broker Interest Received,,,$1.23,Interest",
                "2025-03-03,Other Fees,BABA,,($2.00),ADR fee",
                "2025-01-07,Deposits/Withdrawals,,,($20000.00),DISBURSEMENT INITIATED",
                "2025-02-04,Deposits/Withdrawals,,,($20000.00),DISBURSEMENT INITIATED",
            ]
        ),
    )
    _write(
        data_dir / "chase_positions.csv",
        "\n".join(
            [
                "Symbol,Quantity,Market Value",
                "VTI,5,$1400.00",
                "CASH,100.00,$100.00",
            ]
        ),
    )

    conn = ExternalConnection(
        name="Chase IRA (Offline)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=personal.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.commit()

    r0 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert r0.status == "SUCCESS"

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    assert acct.account_type == "IRA"

    txs = session.query(Transaction).filter(Transaction.account_id == acct.id).all()
    assert len(txs) == 7
    types = {t.type for t in txs}
    assert {"BUY", "SELL", "DIV", "INT", "FEE", "TRANSFER"}.issubset(types)

    # Signs normalized.
    buy = session.query(Transaction).filter(Transaction.account_id == acct.id, Transaction.type == "BUY").first()
    sell = session.query(Transaction).filter(Transaction.account_id == acct.id, Transaction.type == "SELL").first()
    assert buy is not None and float(buy.amount) < 0 and float(buy.qty or 0) > 0
    assert sell is not None and float(sell.amount) > 0 and float(sell.qty or 0) > 0

    # Cash balance imported from holdings.
    cb = session.query(CashBalance).filter(CashBalance.account_id == acct.id).first()
    assert cb is not None
    assert float(cb.amount) == 100.0

    # Re-running FULL is idempotent (no duplicate txns).
    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert r1.status in {"SUCCESS", "PARTIAL"}
    assert session.query(Transaction).filter(Transaction.account_id == acct.id).count() == 7

    # Holdings view shows IRA positions with tax status N/A and no wash-safe exit date.
    view = build_holdings_view(session, scope="personal", account_id=acct.id, today=dt.date(2025, 12, 21))
    vti = next(p for p in view.positions if p.symbol == "VTI")
    assert vti.tax_status == "N/A"
    assert vti.wash_safe_exit_date is None


def test_chase_offline_holdings_fallback_from_transactions_when_no_positions_file(session, tmp_path: Path):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()

    data_dir = tmp_path / "chase2"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write(
        data_dir / "activity_only.csv",
        "\n".join(
            [
                "Date,Type,Symbol,Quantity,Amount,Description",
                "2025-01-10,BUY,VTI,10,($2500.00),Buy VTI",
                "2025-02-10,SELL,VTI,5,$1300.00,Sell VTI",
            ]
        ),
    )

    conn = ExternalConnection(
        name="Chase IRA (Offline 2)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=personal.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.commit()

    r0 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert r0.status == "SUCCESS"
    assert int(r0.coverage_json.get("holdings_items_imported") or 0) >= 1

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    view = build_holdings_view(session, scope="personal", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert any(p.symbol == "VTI" and float(p.qty or 0.0) == 5.0 for p in view.positions)


def test_ira_excluded_from_reconstructed_wash_risk(session):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()
    ira = Account(name="Chase IRA", broker="CHASE", account_type="IRA", taxpayer_entity_id=personal.id)
    session.add(ira)
    session.flush()
    # A loss sale with basis details would normally be considered in wash-risk; IRA must be excluded.
    session.add(
        Transaction(
            account_id=ira.id,
            date=dt.date(2025, 12, 10),
            type="SELL",
            ticker="VTI",
            qty=1,
            amount=90,
            lot_links_json={"basis_total": 100},
        )
    )
    session.commit()

    s = wash_risk_summary(session, as_of=dt.date(2025, 12, 21), scope="personal")
    assert s.recent_sell_count == 0


def test_chase_statement_tsv_parses_amount_usd_price_usd_and_tax_withheld(session, tmp_path: Path):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()

    data_dir = tmp_path / "chase_tsv"
    data_dir.mkdir(parents=True, exist_ok=True)
    p = data_dir / "chase_statement.tsv"
    p.write_text(
        "\n".join(
            [
                "\t".join(
                    [
                        "Trade Date",
                        "Type",
                        "Description",
                        "Ticker",
                        "Security Type",
                        "Local Currency",
                        "Price USD",
                        "Quantity",
                        "Amount USD",
                        "Tax Withheld",
                    ]
                ),
                "\t".join(
                    [
                        "12/5/2025",
                        "Buy",
                        "NVIDIA CORP",
                        "NVDA",
                        "Stock",
                        "USD",
                        "182.11",
                        "100",
                        "-18211",
                        "0",
                    ]
                ),
                "\t".join(
                    [
                        "12/15/2025",
                        "Dividend",
                        "TAIWAN SEMI DIV FOREIGN TAX WITHHELD",
                        "TSM",
                        "Stock",
                        "USD",
                        "0",
                        "0",
                        "169.42",
                        "-35.58",
                    ]
                ),
                "\t".join(
                    [
                        "12/16/2025",
                        "DBS",
                        "JPMORGAN IRA DEPOSIT SWEEP INTRA-DAY DEPOSIT",
                        "QCERQ",
                        "Money Market",
                        "USD",
                        "0",
                        "79533",
                        "-79533",
                        "0",
                    ]
                ),
            ]
        ),
        encoding="utf-8",
    )

    conn = ExternalConnection(
        name="Chase TSV",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=personal.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.commit()

    r0 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert r0.status == "SUCCESS"

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    buy = session.query(Transaction).filter(Transaction.account_id == acct.id, Transaction.type == "BUY", Transaction.ticker == "NVDA").one()
    assert float(buy.amount) == -18211.0

    div = session.query(Transaction).filter(Transaction.account_id == acct.id, Transaction.type == "DIV", Transaction.ticker == "TSM").one()
    assert float(div.amount) == 169.42
    withh = session.query(Transaction).filter(Transaction.account_id == acct.id, Transaction.type == "WITHHOLDING", Transaction.ticker == "TSM").one()
    assert float(withh.amount) == 35.58

    # Fallback inferred holdings includes cash from money market sweep qty.
    assert int(r0.coverage_json.get("holdings_items_imported") or 0) >= 1
    view = build_holdings_view(session, scope="personal", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert any(p.symbol == "CASH:USD" for p in view.positions) or float(view.cash_total) != 0.0


def test_chase_positions_tsv_with_commas_in_values_imports_holdings_and_cash(session, tmp_path: Path):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()

    data_dir = tmp_path / "chase_pos_tsv"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Positions export: tab-delimited with commas in numeric values.
    (data_dir / "positions.tsv").write_text(
        "\n".join(
            [
                "\t".join(["Asset Class", "Description", "Ticker", "Quantity", "Value", "Cost", "As of"]),
                "\t".join(["Equity", "PALANTIR TECHNOLOGIES INC", "PLTR", "900", "174,582", "157,932.5", "12/22/2025"]),
                "\t".join(
                    [
                        "Fixed Income & Cash",
                        "JPMORGAN IRA DEPOSIT SWEEP JPMORGAN CHASE BANK NA",
                        "QCERQ",
                        "122,592.77",
                        "122,592.77",
                        "122,592.77",
                        "12/22/2025",
                    ]
                ),
                "\t".join(["Fixed Income & Cash", "US DOLLAR", "", "-47,772.64", "-47,772.64", "0", "12/22/2025"]),
            ]
        ),
        encoding="utf-8",
    )
    # Minimal activity file so sync has something to page through.
    (data_dir / "activity.csv").write_text(
        "\n".join(
            [
                "Date,Type,Symbol,Quantity,Amount,Description",
                "2025-01-10,BUY,PLTR,10,($1000.00),Buy PLTR",
            ]
        ),
        encoding="utf-8",
    )

    conn = ExternalConnection(
        name="Chase IRA (Offline TSV)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=personal.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.commit()

    r0 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        actor="test",
    )
    assert r0.status == "SUCCESS"
    assert int(r0.coverage_json.get("holdings_items_imported") or 0) >= 1

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    # CashBalance should reflect cash-like rows (sweep + USD line).
    cb = session.query(CashBalance).filter(CashBalance.account_id == acct.id).order_by(CashBalance.id.desc()).first()
    assert cb is not None
    assert abs(float(cb.amount) - (122592.77 - 47772.64)) < 0.02

    view = build_holdings_view(session, scope="personal", account_id=acct.id, today=dt.date(2025, 12, 22))
    pltr = next(p for p in view.positions if p.symbol == "PLTR")
    assert float(pltr.qty or 0.0) == 900.0
    assert abs(float(pltr.market_value or 0.0) - 174582.0) < 0.01
    assert abs(float(pltr.cost_basis_total or 0.0) - 157932.5) < 0.01


def test_chase_sweep_rows_not_counted_as_contributions(session, tmp_path: Path):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()

    data_dir = tmp_path / "chase_sweeps"
    data_dir.mkdir(parents=True, exist_ok=True)
    # Transaction export similar to the user-provided statement: WDL/DBS sweeps + BNK distribution transfer.
    (data_dir / "chase_statement.tsv").write_text(
        "\n".join(
            [
                "\t".join(
                    [
                        "Trade Date",
                        "Type",
                        "Description",
                        "Ticker",
                        "Security Type",
                        "Quantity",
                        "Amount USD",
                        "Tran Code",
                        "Tran Code Description",
                    ]
                ),
                "\t".join(
                    [
                        "12/15/2025",
                        "WDL",
                        "JPMORGAN IRA DEPOSIT SWEEP JPMORGAN CHASE BANK NA INTRA-DAY WITHDRWAL",
                        "QCERQ",
                        "Money Market",
                        "-10000",
                        "10000",
                        "WDL",
                        "",
                    ]
                ),
                "\t".join(["12/15/2025", "BNK", "BANKLINK ACH PUSH IRA:D2025LEG7 67658826", "", "Other", "0", "-9000", "BNK", ""]),
                "\t".join(["12/15/2025", "TAX", "IRA WITHHOLDING TAX FEDERAL W/H", "", "Other", "0", "-1000", "TAX", ""]),
            ]
        ),
        encoding="utf-8",
    )

    conn = ExternalConnection(
        name="Chase IRA (Offline Sweeps)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=personal.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.commit()

    r0 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert r0.status == "SUCCESS"

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    txs = session.query(Transaction).filter(Transaction.account_id == acct.id).all()
    # WDL sweep should not become TRANSFER; BNK should be TRANSFER and keep negative sign.
    assert any(t.type == "OTHER" and (t.ticker or "").upper() == "QCERQ" for t in txs)
    bnk = next(t for t in txs if t.type == "TRANSFER")
    assert float(bnk.amount) < 0
    assert abs(float(bnk.amount) + 9000.0) < 0.01

    view = build_holdings_view(session, scope="personal", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_contributions == 0.0
    assert view.ytd_withdrawals == 9000.0


def test_chase_sweeps_not_counted_when_tran_code_missing(session, tmp_path: Path):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()

    data_dir = tmp_path / "chase_sweeps_no_code"
    data_dir.mkdir(parents=True, exist_ok=True)
    # Same as prior sweep test, but omit Tran Code column entirely (some exports do this).
    (data_dir / "chase_statement.tsv").write_text(
        "\n".join(
            [
                "\t".join(["Trade Date", "Type", "Description", "Ticker", "Security Type", "Quantity", "Amount USD"]),
                "\t".join(
                    [
                        "12/15/2025",
                        "WDL",
                        "JPMORGAN IRA DEPOSIT SWEEP JPMORGAN CHASE BANK NA INTRA-DAY WITHDRWAL",
                        "QCERQ",
                        "Money Market",
                        "-10000",
                        "10000",
                    ]
                ),
                "\t".join(["12/15/2025", "BNK", "BANKLINK ACH PUSH IRA:D2025LEG7 67658826", "", "Other", "0", "-9000"]),
            ]
        ),
        encoding="utf-8",
    )

    conn = ExternalConnection(
        name="Chase IRA (Offline Sweeps No Code)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=personal.id,
        status="ACTIVE",
        metadata_json={"data_dir": str(data_dir)},
    )
    session.add(conn)
    session.commit()

    r0 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), actor="test")
    assert r0.status == "SUCCESS"

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    txs = session.query(Transaction).filter(Transaction.account_id == acct.id).all()
    # WDL sweep should remain OTHER (not TRANSFER).
    assert any(t.type == "OTHER" and (t.ticker or "").upper() == "QCERQ" for t in txs)
    view = build_holdings_view(session, scope="personal", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_contributions == 0.0
    assert view.ytd_withdrawals == 9000.0
