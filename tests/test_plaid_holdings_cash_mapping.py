from __future__ import annotations

import datetime as dt

from src.adapters.plaid_chase.adapter import PlaidChaseAdapter


class _FakeConn:
    def __init__(self) -> None:
        self.credentials = {"PLAID_ACCESS_TOKEN": "AT", "PLAID_ITEM_ID": "ITEM1"}
        self.metadata_json = {"plaid_env": "sandbox"}
        self.run_settings = {}


class _FakePlaidClient:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def investments_holdings_get(self, *, access_token: str):
        return dict(self._payload)


def test_plaid_holdings_maps_cash_sweep_to_cash_usd(monkeypatch) -> None:
    adapter = PlaidChaseAdapter()
    conn = _FakeConn()
    payload = {
        "accounts": [
            {"account_id": "A1", "type": "investment", "subtype": "ira", "iso_currency_code": "USD"},
        ],
        "securities": [
            # Cash sweep security (e.g., QCERQ).
            {"security_id": "S_CASH", "ticker_symbol": "QCERQ", "name": "JPMORGAN IRA DEPOSIT SWEEP", "type": "cash"},
            # A second cash-like leg without a good classification (falls back to price~1 + qty≈mv).
            {"security_id": "S_DEBIT", "name": "Cash", "type": ""},
        ],
        "holdings": [
            {
                "account_id": "A1",
                "security_id": "S_CASH",
                "quantity": 12261.68,
                "institution_value": 12261.68,
                "institution_price": 1.0,
            },
            {
                "account_id": "A1",
                "security_id": "S_DEBIT",
                "quantity": -7094.00,
                "institution_value": -7094.00,
                "institution_price": 1.0,
            },
        ],
    }
    monkeypatch.setattr(adapter, "_client", lambda _ctx: _FakePlaidClient(payload))

    out = adapter.fetch_holdings(conn, as_of=dt.datetime(2026, 1, 9, 12, 0, 0))
    items = list(out.get("items") or [])
    cash_items = [it for it in items if (it.get("symbol") or "").upper().startswith("CASH:")]
    assert len(cash_items) == 1
    assert cash_items[0]["symbol"] == "CASH:USD"
    assert abs(float(cash_items[0]["market_value"]) - 5167.68) < 1e-6

    cash_balances = list(out.get("cash_balances") or [])
    assert len(cash_balances) == 1
    assert abs(float(cash_balances[0]["amount"]) - 5167.68) < 1e-6

