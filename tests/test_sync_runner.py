from __future__ import annotations

import datetime as dt

import pytest

from src.core.sync_runner import _compute_incremental_range, _negotiate_full_range, run_sync
from src.db.models import ExternalConnection, TaxpayerEntity
from src.importers.adapters import BrokerAdapter, ProviderError, RangeTooLargeError
from src.utils.time import utcnow


class FakeAdapter(BrokerAdapter):
    def __init__(
        self,
        *,
        accounts: list[dict],
        pages: list[list[dict]],
        max_span_days: int | None = None,
        error_on_cursor: str | None = None,
    ):
        self._accounts = accounts
        self._pages = pages
        self._max_span_days = max_span_days
        self._error_on_cursor = error_on_cursor

    @property
    def page_size(self) -> int:
        return 2

    def fetch_accounts(self, connection):
        return self._accounts

    def fetch_transactions(self, connection, start_date, end_date, cursor=None):
        if self._max_span_days is not None:
            if (end_date - start_date).days > self._max_span_days:
                raise RangeTooLargeError("range too large")
        if self._error_on_cursor is not None and cursor == self._error_on_cursor:
            raise ProviderError("boom")
        idx = int(cursor) if cursor is not None else 0
        if idx >= len(self._pages):
            return [], None
        next_cursor = str(idx + 1) if (idx + 1) < len(self._pages) else None
        return self._pages[idx], next_cursor

    def fetch_holdings(self, connection, as_of=None):
        return {"as_of": (as_of or utcnow()).isoformat(), "items": []}

    def test_connection(self, connection):
        return {"ok": True, "message": "ok"}


def _mk_connection(session) -> ExternalConnection:
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="TestConn",
        provider="YODLEE",
        broker="IB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"fixture_dir": "fixtures/yodlee_ib", "account_map": {}},
    )
    session.add(conn)
    session.commit()
    return conn


def test_incremental_range_with_overlap():
    now = dt.date(2025, 12, 20)
    last = dt.datetime(2025, 12, 10, 12, 0, 0)
    start, end = _compute_incremental_range(now=now, last_successful_sync_at=last, overlap_days=7)
    assert end == now
    assert start == dt.date(2025, 12, 3)


def test_full_backfill_fallback_negotiation_shrinks_range(session, monkeypatch):
    conn = _mk_connection(session)
    adapter = FakeAdapter(accounts=[{"provider_account_id": "A1", "name": "IB Taxable"}], pages=[[]], max_span_days=365)
    start, end = _negotiate_full_range(
        adapter=adapter,
        connection=conn,
        requested_start=dt.date(2015, 1, 1),
        requested_end=dt.date(2025, 12, 20),
    )
    assert end == dt.date(2025, 12, 20)
    assert (end - start).days <= 365


def test_pagination_runs_until_exhausted(session, monkeypatch):
    conn = _mk_connection(session)
    adapter = FakeAdapter(
        accounts=[{"provider_account_id": "A1", "name": "IB Taxable"}],
        pages=[
            [{"provider_transaction_id": "T1", "provider_account_id": "A1", "date": "2025-12-01", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100}],
            [{"provider_transaction_id": "T2", "provider_account_id": "A1", "date": "2025-12-02", "type": "SELL", "symbol": "VTI", "qty": 1, "amount": 110}],
            [{"provider_transaction_id": "T3", "provider_account_id": "A1", "date": "2025-12-03", "type": "FEE", "symbol": None, "qty": None, "amount": -1}],
        ],
    )
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 12, 1), end_date=dt.date(2025, 12, 20), actor="test")
    assert run.status == "SUCCESS"
    assert run.coverage_json["pages_fetched"] == 3
    assert run.coverage_json["txn_count"] == 3
    assert run.coverage_json["new_inserted"] == 3


def test_idempotent_import_skips_duplicates(session, monkeypatch):
    conn = _mk_connection(session)
    pages = [
        [{"provider_transaction_id": "T1", "provider_account_id": "A1", "date": "2025-12-01", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100}],
    ]
    adapter = FakeAdapter(accounts=[{"provider_account_id": "A1", "name": "IB Taxable"}], pages=pages)
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)
    r1 = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 12, 1), end_date=dt.date(2025, 12, 20), actor="test")
    r2 = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", overlap_days=0, actor="test")
    assert r1.coverage_json["new_inserted"] == 1
    assert r2.coverage_json["new_inserted"] == 0
    assert r2.coverage_json["txn_count"] >= 1
    assert r2.coverage_json["duplicates_skipped"] >= 1


def test_existing_lot_does_not_prevent_txn_import(session, monkeypatch):
    conn = _mk_connection(session)
    # Seed a lot that would collide with the BUY-lot insertion.
    from src.db.models import Account, PositionLot

    acct = session.query(Account).filter(Account.name == "IB Taxable").one_or_none()
    if acct is None:
        acct = Account(name="IB Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=conn.taxpayer_entity_id)
        session.add(acct)
        session.commit()
    session.add(
        PositionLot(
            account_id=acct.id,
            ticker="VTI",
            acquisition_date=dt.date(2025, 12, 1),
            qty=1,
            basis_total=100,
        )
    )
    session.commit()

    adapter = FakeAdapter(
        accounts=[{"provider_account_id": "A1", "name": "IB Taxable"}],
        pages=[[{"provider_transaction_id": "T1", "provider_account_id": "A1", "date": "2025-12-01", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100}]],
    )
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 12, 1), end_date=dt.date(2025, 12, 20), actor="test")
    assert run.status == "SUCCESS"
    assert run.coverage_json["new_inserted"] == 1


def test_parse_fail_triggers_partial(session, monkeypatch):
    conn = _mk_connection(session)
    adapter = FakeAdapter(
        accounts=[{"provider_account_id": "A1", "name": "IB Taxable"}],
        pages=[
            [
                {"provider_transaction_id": "T1", "provider_account_id": "A1", "date": "bad-date", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100},
                {"provider_transaction_id": "T2", "provider_account_id": "A1", "date": "2025-12-02", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100},
            ]
        ],
    )
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 12, 1), end_date=dt.date(2025, 12, 20), actor="test")
    assert run.status == "PARTIAL"
    assert run.coverage_json["txn_count"] == 2
    assert run.coverage_json["parse_fail_count"] >= 1


def test_provider_error_mid_pagination_triggers_partial(session, monkeypatch):
    conn = _mk_connection(session)
    adapter = FakeAdapter(
        accounts=[{"provider_account_id": "A1", "name": "IB Taxable"}],
        pages=[
            [{"provider_transaction_id": "T1", "provider_account_id": "A1", "date": "2025-12-01", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100}],
            [{"provider_transaction_id": "T2", "provider_account_id": "A1", "date": "2025-12-02", "type": "BUY", "symbol": "VTI", "qty": 1, "amount": -100}],
        ],
        error_on_cursor="1",
    )
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)
    run = run_sync(session, connection_id=conn.id, mode="FULL", start_date=dt.date(2025, 12, 1), end_date=dt.date(2025, 12, 20), actor="test")
    assert run.status == "PARTIAL"


def test_zero_accounts_is_error(session, monkeypatch):
    conn = _mk_connection(session)
    adapter = FakeAdapter(accounts=[], pages=[[]])
    monkeypatch.setattr("src.core.sync_runner._adapter_for", lambda _c: adapter)
    run = run_sync(session, connection_id=conn.id, mode="INCREMENTAL", actor="test")
    assert run.status == "ERROR"
