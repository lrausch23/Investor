from __future__ import annotations

import datetime as dt
import json

from starlette.requests import Request

from src.app.routes.api_native import native_holdings_drilldown
from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, PositionLot, Security, TaxpayerEntity


def _request(query: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/native/holdings/drilldown",
        "query_string": query.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "scheme": "http",
        "server": ("test", 80),
    }
    return Request(scope)


def test_api_native_holdings_drilldown_returns_lots(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()

    acct = Account(name="Trust Account", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.add(Security(ticker="AAA", name="AAA", asset_class="EQUITY", metadata_json={"last_price": 120}))
    session.flush()
    conn = ExternalConnection(
        name="IB Native",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="U1", account_id=acct.id))
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2026, 2, 7, 12, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "U1", "symbol": "AAA", "qty": 10, "market_value": 1200}]},
        )
    )

    session.add(
        PositionLot(
            account_id=acct.id,
            ticker="AAA",
            acquisition_date=dt.date(2024, 1, 2),
            qty=10,
            basis_total=1000,
        )
    )
    session.commit()

    response = native_holdings_drilldown(
        request=_request(f"scope=trust&account_id={acct.id}&symbol=AAA"),
        session=session,
        actor="native-user",
    )

    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["scope"] == "trust"
    assert payload["account_id"] == acct.id
    assert payload["symbol"] == "AAA"
    assert payload["lots_source"] in {"position_lots", "tax_lots"}
    assert len(payload["lots"]) >= 1
