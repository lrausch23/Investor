from __future__ import annotations

import datetime as dt
import os

import pytest

from src.core.credential_store import CredentialError, mask_secret, upsert_credential
from src.core.sync_coverage import compute_coverage_status
from src.core.sync_runner import run_sync
from src.db.models import ExternalConnection, SyncRun, TaxpayerEntity
from src.importers.adapters import BrokerAdapter
from src.utils.time import utcnow


class MinimalAdapter(BrokerAdapter):
    def __init__(self, accounts, pages):
        self._accounts = accounts
        self._pages = pages

    @property
    def page_size(self) -> int:
        return 50

    def fetch_accounts(self, connection):
        return self._accounts

    def fetch_transactions(self, connection, start_date, end_date, cursor=None):
        idx = int(cursor) if cursor is not None else 0
        if idx >= len(self._pages):
            return [], None
        next_cursor = str(idx + 1) if (idx + 1) < len(self._pages) else None
        return self._pages[idx], next_cursor

    def fetch_holdings(self, connection, as_of=None):
        return {"as_of": (as_of or utcnow()).isoformat(), "items": []}

    def test_connection(self, connection):
        return {"ok": True, "message": "ok"}


def test_coverage_status_unknown_partial_complete(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(name="C", provider="YODLEE", broker="IB", taxpayer_entity_id=tp.id, status="ACTIVE", metadata_json={})
    session.add(conn)
    session.flush()

    assert compute_coverage_status(conn, None) == "UNKNOWN"

    conn.last_full_sync_at = dt.datetime(2025, 1, 1)
    partial_run = SyncRun(connection_id=conn.id, status="PARTIAL", mode="FULL")
    assert compute_coverage_status(conn, partial_run) == "PARTIAL"

    conn.last_error_json = '{"error":"x"}'
    success_run = SyncRun(connection_id=conn.id, status="SUCCESS", mode="INCREMENTAL", parse_fail_count=0)
    assert compute_coverage_status(conn, success_run) == "PARTIAL"

    conn.last_error_json = None
    assert compute_coverage_status(conn, success_run) == "COMPLETE"


def test_full_backfill_sets_earliest_available_and_last_full_sync(session, monkeypatch):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="C",
        provider="YODLEE",
        broker="IB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"fixture_dir": "fixtures/yodlee_ib"},
    )
    session.add(conn)
    session.commit()

    adapter = MinimalAdapter(
        accounts=[{"provider_account_id": "A1", "name": "IB Taxable", "account_type": "TAXABLE"}],
        pages=[
            [{"provider_transaction_id": "T1", "provider_account_id": "A1", "date": "2020-01-01", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100}],
            [{"provider_transaction_id": "T2", "provider_account_id": "A1", "date": "2022-01-01", "type": "SELL", "symbol": "VTI", "qty": 1, "amount": 110}],
        ],
    )
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)

    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2015, 1, 1),
        end_date=dt.date(2025, 1, 1),
        actor="test",
    )
    assert run.status == "SUCCESS"

    session.refresh(conn)
    assert conn.last_full_sync_at is not None
    assert conn.txn_earliest_available == dt.date(2020, 1, 1)
    assert conn.coverage_status == "COMPLETE"


def test_auth_save_blocked_without_secret_key(session, monkeypatch):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(name="C", provider="YODLEE", broker="IB", taxpayer_entity_id=tp.id, status="ACTIVE", metadata_json={})
    session.add(conn)
    session.commit()

    prev = os.environ.get("APP_SECRET_KEY")
    if "APP_SECRET_KEY" in os.environ:
        del os.environ["APP_SECRET_KEY"]
    try:
        with pytest.raises(CredentialError):
            upsert_credential(session, connection_id=conn.id, key="IB_YODLEE_TOKEN", plaintext="secret")
    finally:
        if prev is not None:
            os.environ["APP_SECRET_KEY"] = prev


def test_mask_secret_shows_last4():
    assert mask_secret("ABCDEFGHIJ") == "**********GHIJ"
    assert mask_secret("") == "—"
    assert mask_secret(None) == "—"
