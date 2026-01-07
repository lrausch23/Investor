from __future__ import annotations

import csv
import datetime as dt

from src.core.monthly_report_source import build_monthly_report_inputs_from_db
from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity, Transaction


def _rows(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(text.splitlines()))


def test_monthly_report_inputs_from_db_basic(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()

    acct = Account(name="RJ Taxable", broker="RJ", taxpayer_entity_id=tp.id, account_type="TAXABLE")
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="RJ (Offline)",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()

    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="RJ:TAXABLE", account_id=acct.id))
    session.flush()

    def snap(d: dt.date, mv: float):
        return ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=dt.timezone.utc),
            payload_json={"as_of": f"{d.isoformat()}T23:59:59+00:00", "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "AAA", "quantity": 1, "market_value": mv}]},
        )

    session.add_all(
        [
            snap(dt.date(2024, 12, 31), 100.0),
            snap(dt.date(2025, 1, 31), 110.0),
            snap(dt.date(2025, 2, 28), 120.0),
        ]
    )
    session.flush()

    session.add_all(
        [
            Transaction(account_id=acct.id, date=dt.date(2025, 1, 15), type="TRANSFER", ticker=None, qty=None, amount=50.0, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 2, 10), type="TRANSFER", ticker=None, qty=None, amount=-10.0, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 1, 20), type="FEE", ticker=None, qty=None, amount=-1.0, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 2, 12), type="WITHHOLDING", ticker=None, qty=None, amount=2.0, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 1, 25), type="DIV", ticker="AAA", qty=None, amount=3.0, lot_links_json={}),
        ]
    )
    session.commit()

    inputs = build_monthly_report_inputs_from_db(
        session,
        scope="trust",
        connection_id=conn.id,
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 2, 28),
        asof_date=dt.date(2025, 2, 28),
        grace_days=14,
    )
    assert inputs.transactions_csv_bytes
    assert inputs.monthly_perf_csv_bytes

    monthly = _rows(inputs.monthly_perf_csv_bytes.decode("utf-8"))
    assert len(monthly) == 2
    jan = monthly[0]
    feb = monthly[1]
    assert jan["Date"] == "2025-01-31"
    assert float(jan["Beginning market value"]) == 100.0
    assert float(jan["Ending market value"]) == 110.0
    assert float(jan["Contributions"]) == 50.0
    assert float(jan["Withdrawals"]) == 0.0
    assert float(jan["Taxes withheld"]) == 0.0
    assert float(jan["Fees"]) == 1.0
    assert float(jan["Income"]) == 3.0

    assert feb["Date"] == "2025-02-28"
    assert float(feb["Beginning market value"]) == 110.0
    assert float(feb["Ending market value"]) == 120.0
    assert float(feb["Contributions"]) == 0.0
    assert float(feb["Withdrawals"]) == 10.0
    assert float(feb["Taxes withheld"]) == 2.0
    assert float(feb["Fees"]) == 0.0

