from __future__ import annotations

import datetime as dt
import shutil
from pathlib import Path

from src.core.sync_runner import run_sync
from src.db.models import (
    BrokerLotClosure,
    BrokerWashSaleEvent,
    ExternalConnection,
    TaxpayerEntity,
    Transaction,
)


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


def test_trades_multidetail_imports_closed_lots_and_wash_sales(session, tmp_path: Path):
    src_dir = Path("fixtures/ib_flex_offline")
    work = tmp_path / "ibflex"
    work.mkdir(parents=True, exist_ok=True)
    shutil.copy(src_dir / "ib_lots_trimmed.csv", work / "IB_Lots.csv")

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
    # EXECUTION rows become Transactions
    assert session.query(Transaction).count() == 2
    # CLOSED_LOT rows
    assert session.query(BrokerLotClosure).count() == 1
    cl = session.query(BrokerLotClosure).first()
    assert cl is not None
    assert float(cl.cost_basis or 0.0) == 1000.0
    assert float(cl.realized_pl_fifo or 0.0) == -200.0
    assert float(cl.proceeds_derived or 0.0) == 800.0
    # WASH_SALE rows (one links to CLOSED_LOT and gets proceeds/basis; one remains unlinked)
    assert session.query(BrokerWashSaleEvent).count() == 2
    rows = session.query(BrokerWashSaleEvent).order_by(BrokerWashSaleEvent.id.asc()).all()
    assert any(r.linked_closure_id is not None for r in rows)
    assert any(r.linked_closure_id is None for r in rows)
    linked = next(r for r in rows if r.linked_closure_id is not None)
    assert float(linked.proceeds_derived or 0.0) == 800.0
    assert float(linked.disallowed_loss or 0.0) == 200.0
