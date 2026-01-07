from __future__ import annotations

import datetime as dt

import pytest

from src.core.credential_store import upsert_credential
from src.core.sync_runner import run_sync
from src.core.wash_sale import wash_risk_for_loss_sale
from src.db.models import Account, CashBalance, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity, Transaction


class _FakeYodleeClient:
    def __init__(self, *, state: dict):
        self._state = state
        self.rate_limit_hits = 0

    def get_accounts(self) -> dict:
        return self._state["accounts_payload"]

    def get_holdings(self, *, account_id: str) -> dict:
        return self._state["holdings_by_account"].get(account_id) or {"holding": []}

    def get_transactions(self, *, account_id: str, start_date: str, end_date: str, skip: int, top: int) -> dict:
        rows = list(self._state["txns_by_account"].get(account_id) or [])
        return {"transaction": rows[skip : skip + top]}


def _mk_conn(session) -> ExternalConnection:
    tp = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="Chase IRA (Yodlee)",
        provider="YODLEE",
        broker="CHASE",
        connector="CHASE_YODLEE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"yodlee_base_url": "https://api.yodlee.example"},
    )
    session.add(conn)
    session.commit()
    return conn


def test_chase_yodlee_full_sync_imports_holdings_and_transactions(session, monkeypatch):
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("NETWORK_ENABLED", "1")

    conn = _mk_conn(session)
    upsert_credential(session, connection_id=conn.id, key="YODLEE_ACCESS_TOKEN", plaintext="TOK")
    session.commit()

    state = {
        "accounts_payload": {
            "account": [
                {"id": "A1", "accountName": "Chase IRA", "accountType": "IRA", "providerName": "Chase"},
            ]
        },
        "holdings_by_account": {
            "A1": {
                "holding": [
                    {"symbol": "VTI", "quantity": 10, "marketValue": 1000, "costBasis": 900},
                    {"symbol": "CASH", "marketValue": 200},
                ]
            }
        },
        "txns_by_account": {
            "A1": [
                {
                    "id": "T_CONTRIB",
                    "transactionDate": "2025-01-02",
                    "description": "IRA Contribution",
                    "amount": {"amount": 500, "currency": "USD"},
                    "baseType": "CREDIT",
                    "categoryType": "CONTRIBUTION",
                },
                {
                    "id": "T_BUY",
                    "transactionDate": "2025-01-03",
                    "description": "BUY VTI",
                    "amount": {"amount": 100, "currency": "USD"},
                    "baseType": "DEBIT",
                    "transactionType": "BUY",
                    "symbol": "VTI",
                    "quantity": 1,
                },
            ]
        },
    }

    from src.adapters.yodlee_chase import adapter as mod

    def fake_client(self, ctx):
        c = (ctx.run_settings or {}).get("_fake_client")
        if c is None:
            c = _FakeYodleeClient(state=state)
            ctx.run_settings["_fake_client"] = c
        return c

    monkeypatch.setattr(mod.YodleeChaseAdapter, "_client", fake_client)

    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 10),
        actor="test",
    )
    assert run.status == "SUCCESS"

    acct = session.query(Account).filter(Account.name == "Chase IRA").one()
    assert acct.account_type == "IRA"

    txs = session.query(Transaction).order_by(Transaction.date.asc(), Transaction.id.asc()).all()
    assert len(txs) == 2
    assert {t.type for t in txs} == {"TRANSFER", "BUY"}

    hs = session.query(ExternalHoldingSnapshot).filter(ExternalHoldingSnapshot.connection_id == conn.id).one()
    items = list((hs.payload_json or {}).get("items") or [])
    assert any((it.get("symbol") or "").upper() == "VTI" for it in items)

    cb = session.query(CashBalance).filter(CashBalance.account_id == acct.id).one_or_none()
    assert cb is not None
    assert cb.amount == pytest.approx(200.0)

    # IRA should be excluded from wash-sale scope (wash_risk only considers TAXABLE accounts).
    risk, matches = wash_risk_for_loss_sale(
        session,
        taxpayer_entity_id=acct.taxpayer_entity_id,
        sale_ticker="VTI",
        sale_date=dt.date(2025, 1, 4),
        proposed_buys=[],
    )
    assert risk == "NONE"
    assert matches == []


def test_chase_yodlee_incremental_is_idempotent(session, monkeypatch):
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("NETWORK_ENABLED", "1")

    conn = _mk_conn(session)
    upsert_credential(session, connection_id=conn.id, key="YODLEE_ACCESS_TOKEN", plaintext="TOK")
    session.commit()

    state = {
        "accounts_payload": {"account": [{"id": "A1", "accountName": "Chase IRA", "accountType": "IRA", "providerName": "Chase"}]},
        "holdings_by_account": {"A1": {"holding": [{"symbol": "VTI", "quantity": 10, "marketValue": 1000, "costBasis": 900}]}},
        "txns_by_account": {"A1": [{"id": "T1", "transactionDate": "2025-01-02", "description": "Fee", "amount": {"amount": 1, "currency": "USD"}, "baseType": "DEBIT", "categoryType": "FEE"}]},
    }

    from src.adapters.yodlee_chase import adapter as mod

    def fake_client(self, ctx):
        c = (ctx.run_settings or {}).get("_fake_client")
        if c is None:
            c = _FakeYodleeClient(state=state)
            ctx.run_settings["_fake_client"] = c
        return c

    monkeypatch.setattr(mod.YodleeChaseAdapter, "_client", fake_client)

    r1 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 1, 10), actor="test")
    assert r1.status == "SUCCESS"
    assert r1.coverage_json["new_inserted"] == 1

    # Second run adds a new txn while repeating the old one.
    state["txns_by_account"]["A1"] = [
        {"id": "T1", "transactionDate": "2025-01-02", "description": "Fee", "amount": {"amount": 1, "currency": "USD"}, "baseType": "DEBIT", "categoryType": "FEE"},
        {"id": "T2", "transactionDate": "2025-01-03", "description": "Dividend", "amount": {"amount": 2, "currency": "USD"}, "baseType": "CREDIT", "categoryType": "DIVIDEND"},
    ]
    r2 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", overlap_days=0, actor="test")
    assert r2.status == "SUCCESS"
    assert r2.coverage_json["new_inserted"] == 1
    assert r2.coverage_json["duplicates_skipped"] >= 1
    assert session.query(Transaction).count() == 2


def test_chase_yodlee_unauthorized_fails_cleanly(session, monkeypatch):
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("NETWORK_ENABLED", "1")

    conn = _mk_conn(session)
    upsert_credential(session, connection_id=conn.id, key="YODLEE_ACCESS_TOKEN", plaintext="TOK")
    session.commit()

    from src.adapters.yodlee_chase import adapter as mod
    from src.importers.adapters import ProviderError

    class _UnauthorizedClient:
        rate_limit_hits = 0

        def get_accounts(self):
            raise ProviderError("Unauthorized; token expired/invalid. Update credentials and try again.")

    monkeypatch.setattr(mod.YodleeChaseAdapter, "_client", lambda _self, _ctx: _UnauthorizedClient())

    run = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", actor="test")
    assert run.status == "ERROR"
    assert "Unauthorized" in (run.error_json or "")


def test_yodlee_client_retries_on_429(monkeypatch):
    from src.adapters.yodlee_chase.client import HttpResponse, YodleeChaseClient

    calls = {"n": 0}

    def transport(url: str, method: str, headers: dict, body: bytes | None, timeout_s: float) -> HttpResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            return HttpResponse(status_code=429, content=b"{}", headers={"Retry-After": "0"})
        return HttpResponse(status_code=200, content=b'{"account":[{"id":"A1","accountName":"Chase IRA"}]}', headers={})

    client = YodleeChaseClient(
        base_url="https://api.yodlee.example",
        access_token="TOK",
        transport=transport,
        sleep_fn=lambda _s: None,
        max_retries=2,
    )
    data = client.get_accounts()
    assert calls["n"] == 2
    assert client.rate_limit_hits == 1
    assert isinstance(data, dict)
