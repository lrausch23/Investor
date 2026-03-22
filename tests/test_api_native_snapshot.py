from __future__ import annotations

import json

from starlette.requests import Request

from src.app.routes.api_native import native_snapshot
from src.db.models import TaxpayerEntity


def _request(query: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/native/snapshot",
        "query_string": query.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "scheme": "http",
        "server": ("test", 80),
    }
    return Request(scope)


def test_api_native_snapshot_route_function_returns_json(session):
    session.add(TaxpayerEntity(name="Trust", type="TRUST"))
    session.commit()

    response = native_snapshot(request=_request("scope=trust"), session=session, actor="test-user")

    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["scope"] == "trust"
    assert payload["actor"] == "test-user"
    assert "kpis" in payload
    assert "holdings" in payload
