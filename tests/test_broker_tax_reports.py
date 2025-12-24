from __future__ import annotations

import datetime as dt
import shutil
from pathlib import Path

from src.core.broker_tax import broker_realized_gains, broker_tax_summary
from src.core.sync_runner import run_sync
from src.db.models import (
    BrokerLotClosure,
    BrokerWashSaleEvent,
    ExternalConnection,
    TaxAssumptionsSet,
    TaxpayerEntity,
)


def _mk_conn(session, *, data_dir: str) -> ExternalConnection:
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Offline (Tax Sample)",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"data_dir": data_dir},
    )
    session.add(conn)
    session.flush()
    # Provide rates for predictable tax calc in tests.
    session.add(
        TaxAssumptionsSet(
            name="Default",
            effective_date=dt.date(2025, 1, 1),
            json_definition={
                "ordinary_rate": 0.37,
                "ltcg_rate": 0.20,
                "state_rate": 0.0,
                "niit_enabled": False,
                "niit_rate": 0.038,
                "qualified_dividend_pct": 0.0,
                "tax_rates": {"trust": {"st_rate": 0.50, "lt_rate": 0.20}},
            },
        )
    )
    session.commit()
    return conn


def test_broker_wash_rows_link_to_closures_and_tax_summary(session, tmp_path: Path):
    src = Path("fixtures/ib_flex_offline/ib_lots_tax_sample.csv")
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, work / "IB_Lots.csv")

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

    assert session.query(BrokerLotClosure).count() == 2
    assert session.query(BrokerWashSaleEvent).count() == 1

    ws = session.query(BrokerWashSaleEvent).first()
    assert ws is not None
    assert ws.linked_closure_id is not None
    assert float(ws.basis_effective or 0.0) == 1000.0
    assert float(ws.proceeds_derived or 0.0) == 800.0
    assert float(ws.disallowed_loss or 0.0) == 200.0

    summary, _by_symbol, _detail, _cov = broker_realized_gains(session, scope="trust", year=2025)
    # AAPL: -200, MSFT: +100 => total realized = -100
    assert round(summary.realized, 6) == -100.0

    tax = broker_tax_summary(session, scope="trust", year=2025)
    assert len(tax["rows"]) == 1
    row = tax["rows"][0]
    assert round(float(row["realized_total"]), 6) == -100.0
    assert round(float(row["disallowed_loss"]), 6) == 200.0
    # Conservative: net_taxable = realized + disallowed = 100
    assert round(float(row["net_taxable"]), 6) == 100.0
    # Additional tax due: MSFT is LT in fixture => 100 * 0.20 = 20
    assert round(float(row["additional_tax_due"]), 6) == 20.0
