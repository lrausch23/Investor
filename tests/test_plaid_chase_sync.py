from __future__ import annotations

import datetime as dt

import pytest

from src.core.credential_store import upsert_credential
from src.core.sync_runner import run_sync
from src.db.models import Account, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction, ExternalConnection, TaxpayerEntity, Transaction


class _FakePlaidClient:
    def __init__(self, *, state: dict):
        self._state = state
        self.inv_calls: list[tuple[dt.date, dt.date]] = []

    def get_accounts(self, *, access_token: str):
        return list(self._state["accounts"])

    def transactions_sync(self, *, access_token: str, cursor: str | None, count: int = 500):
        # cursor drives a simple scripted response.
        return self._state["sync_by_cursor"].get(cursor or "") or {"added": [], "modified": [], "removed": [], "has_more": False, "next_cursor": cursor or ""}

    def investments_transactions_get(self, *, access_token: str, start_date: dt.date, end_date: dt.date, offset: int = 0, count: int = 500):
        self.inv_calls.append((start_date, end_date))
        # Support basic pagination by offset.
        txns = list(self._state.get("investment_transactions") or [])
        page = txns[offset : offset + count]
        return {
            "investment_transactions": page,
            "securities": list(self._state.get("investment_securities") or []),
            "total_investment_transactions": len(txns),
        }

    def investments_holdings_get(self, *, access_token: str):
        # Keep empty for these tests; the connector is used for Expenses.
        return {"accounts": [], "holdings": [], "securities": []}


def _mk_conn(session) -> ExternalConnection:
    tp = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="Chase (Plaid)",
        provider="PLAID",
        broker="CHASE",
        connector="CHASE_PLAID",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"plaid_env": "sandbox"},
    )
    session.add(conn)
    session.commit()
    return conn


def test_plaid_chase_sync_writes_expenses_and_persists_cursor(session, monkeypatch):
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("NETWORK_ENABLED", "1")

    conn = _mk_conn(session)
    upsert_credential(session, connection_id=conn.id, key="PLAID_ACCESS_TOKEN", plaintext="AT")
    upsert_credential(session, connection_id=conn.id, key="PLAID_ITEM_ID", plaintext="ITEM1")
    session.commit()

    state = {
        "accounts": [
            {
                "account_id": "A1",
                "name": "Chase Checking",
                "official_name": "Chase Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            }
        ],
        "sync_by_cursor": {
            "": {
                "added": [
                    {"transaction_id": "T1", "account_id": "A1", "date": "2026-01-01", "amount": 12.34, "iso_currency_code": "USD", "name": "Coffee"},
                    {"transaction_id": "T2", "account_id": "A1", "date": "2026-01-02", "amount": -1000.00, "iso_currency_code": "USD", "name": "Payroll"},
                ],
                "modified": [],
                "removed": [],
                "has_more": False,
                "next_cursor": "CUR1",
            },
            "CUR1": {
                "added": [
                    {"transaction_id": "T3", "account_id": "A1", "date": "2026-01-03", "amount": 5.00, "iso_currency_code": "USD", "name": "Snack"},
                ],
                "modified": [],
                "removed": [],
                "has_more": False,
                "next_cursor": "CUR2",
            },
        },
    }

    from src.adapters.plaid_chase import adapter as mod

    monkeypatch.setattr(mod.PlaidChaseAdapter, "_client", lambda _self, _ctx: _FakePlaidClient(state=state))

    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 5),
        pull_holdings=False,
        actor="test",
    )
    assert r1.status == "SUCCESS"
    assert session.query(Transaction).count() == 0

    acct = session.query(ExpenseAccount).filter(ExpenseAccount.name == "Chase Checking").one()
    assert acct.last4_masked == "1234"
    assert session.query(ExpenseTransaction).count() == 2
    batch = session.query(ExpenseImportBatch).one()
    assert batch.source == "PLAID"
    assert batch.row_count == 2

    conn2 = session.query(ExternalConnection).filter(ExternalConnection.id == conn.id).one()
    assert (conn2.metadata_json or {}).get("plaid_transactions_cursor") == "CUR1"

    # Incremental uses persisted cursor and only adds new txns.
    r2 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", overlap_days=0, pull_holdings=False, actor="test")
    assert r2.status == "SUCCESS"
    assert session.query(ExpenseTransaction).count() == 3
    conn3 = session.query(ExternalConnection).filter(ExternalConnection.id == conn.id).one()
    assert (conn3.metadata_json or {}).get("plaid_transactions_cursor") == "CUR2"


def test_plaid_chase_investments_one_time_24m_backfill(session, monkeypatch):
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("NETWORK_ENABLED", "1")

    # Freeze time so the 24m window is deterministic.
    import datetime as _dt

    from src.core import sync_runner as sr

    fixed_now = _dt.datetime(2026, 1, 9, 12, 0, 0, tzinfo=_dt.timezone.utc)
    monkeypatch.setattr(sr, "utcnow", lambda: fixed_now)

    conn = _mk_conn(session)
    conn.metadata_json = {"plaid_env": "sandbox", "plaid_enable_investments": True}
    session.add(conn)
    session.commit()

    upsert_credential(session, connection_id=conn.id, key="PLAID_ACCESS_TOKEN", plaintext="AT")
    upsert_credential(session, connection_id=conn.id, key="PLAID_ITEM_ID", plaintext="ITEM1")
    session.commit()

    state = {
        "accounts": [
            {
                "account_id": "INV1",
                "name": "Chase IRA",
                "official_name": "Chase IRA",
                "type": "investment",
                "subtype": "ira",
                "mask": "8839",
            },
            {
                "account_id": "D1",
                "name": "Chase Checking",
                "official_name": "Chase Checking",
                "type": "depository",
                "subtype": "checking",
                "mask": "1234",
            },
        ],
        "sync_by_cursor": {
            # First run: historical still in progress.
            "": {
                "added": [
                    # Depository txn -> Expenses.
                    {"transaction_id": "TD1", "account_id": "D1", "date": "2026-01-02", "amount": 12.34, "iso_currency_code": "USD", "name": "Coffee"},
                    # Brokerage->bank deposit (inflow, Plaid sign convention is often negative for inflow).
                    {"transaction_id": "TD2", "account_id": "D1", "date": "2026-01-02", "amount": -4500.00, "iso_currency_code": "USD", "name": "Manual CR-Bkrg"},
                    # Investment-account cashflows that are NOT present in /investments/transactions/get for Chase:
                    # - Withholding tax (outflow)
                    {"transaction_id": "TI1", "account_id": "INV1", "date": "2026-01-02", "amount": 500.00, "iso_currency_code": "USD", "name": "IRA WITHHOLDING TAX FEDERAL W/H LEGAL"},
                    # - Transfer out to bank/checking (outflow)
                    {"transaction_id": "TI2", "account_id": "INV1", "date": "2026-01-02", "amount": 4500.00, "iso_currency_code": "USD", "name": "BANKLINK ACH PUSH IRA:D2026LEG7"},
                ],
                "modified": [],
                "removed": [],
                "has_more": False,
                "next_cursor": "CUR1",
                "transactions_update_status": "HISTORICAL_UPDATE_IN_PROGRESS",
            },
            # Second run: historical complete.
            "CUR1": {"added": [], "modified": [], "removed": [], "has_more": False, "next_cursor": "CUR2", "transactions_update_status": "HISTORICAL_UPDATE_COMPLETE"},
        },
        "investment_securities": [{"security_id": "S1", "ticker_symbol": "SPY", "name": "SPDR S&P 500 ETF"}],
        "investment_transactions": [
            {
                "investment_transaction_id": "IT1",
                "account_id": "INV1",
                "security_id": "S1",
                "date": "2025-01-15",
                "type": "buy",
                "amount": 100.0,
                "quantity": 1.0,
                "price": 100.0,
                "iso_currency_code": "USD",
                "name": "BUY SPY",
            }
        ],
    }

    from src.adapters.plaid_chase import adapter as mod

    client = _FakePlaidClient(state=state)
    monkeypatch.setattr(mod.PlaidChaseAdapter, "_client", lambda _self, _ctx: client)

    # First incremental run triggers 24m investment backfill (date-range based).
    r1 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", pull_holdings=False, actor="test")
    assert r1.status == "SUCCESS"
    assert client.inv_calls, "Expected /investments/transactions/get to be called"
    assert client.inv_calls[0][0] == (fixed_now.date() - _dt.timedelta(days=730))
    assert client.inv_calls[0][1] == fixed_now.date()

    conn1 = session.query(ExternalConnection).filter(ExternalConnection.id == conn.id).one()
    # Investments backfill should be marked done after a successful fetch (even if historical bank sync isn't complete yet).
    assert (conn1.metadata_json or {}).get("plaid_investments_backfill_done") is True
    # Bank historical backfill remains incomplete until Plaid reports "complete".
    assert (conn1.metadata_json or {}).get("plaid_initial_backfill_done") in (None, False)

    # Investment-account `/transactions/sync` cashflows should go to the investment `transactions` ledger,
    # not into Expenses.
    ira_acct = session.query(Account).filter_by(broker="CHASE", account_type="IRA").one()
    ira = session.query(Transaction).filter(Transaction.account_id == ira_acct.id).all()
    assert any(t.type == "WITHHOLDING" and float(t.amount) == 500.0 for t in ira)
    assert any(t.type == "TRANSFER" and float(t.amount) == -4500.0 for t in ira)

    # Depository `/transactions/sync` rows still go to Expenses.
    assert session.query(ExpenseTransaction).count() == 2

    # Checking brokerage credit should infer a matching IRA cash-out transfer (negative) for cash-out reporting.
    assert any(t.type == "TRANSFER" and float(t.amount) == -4500.0 for t in ira)

    # Second incremental run should NOT re-run a 24m investment backfill (should use a small incremental window).
    r2 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", pull_holdings=False, actor="test")
    assert r2.status == "SUCCESS"
    assert len(client.inv_calls) >= 2
    assert client.inv_calls[1][0] == (fixed_now.date() - _dt.timedelta(days=7))
    assert client.inv_calls[1][1] == fixed_now.date()

    conn2 = session.query(ExternalConnection).filter(ExternalConnection.id == conn.id).one()
    assert (conn2.metadata_json or {}).get("plaid_initial_backfill_done") is True
