from __future__ import annotations

import datetime as dt
import os

import pytest

from src.core.credential_store import upsert_credential
from src.core.sync_runner import AdapterConnectionContext, run_sync
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerWashSaleEvent,
    ExternalConnection,
    ExternalHoldingSnapshot,
    ExternalTransactionMap,
    TaxpayerEntity,
    Transaction,
)


SEND_OK = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
  <Status>Success</Status>
  <ReferenceCode>REF123</ReferenceCode>
</FlexStatementResponse>
"""

SEND_1018 = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
  <Status>Fail</Status>
  <ErrorCode>1018</ErrorCode>
  <ErrorMessage>Too many requests have been made from this token.</ErrorMessage>
</FlexStatementResponse>
"""

GET_USER_INFO = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
  <Status>Success</Status>
  <UserInfo>
    <FlexQueries>
      <FlexQuery id="123" name="QID" />
    </FlexQueries>
  </UserInfo>
</FlexStatementResponse>
"""

NOT_READY = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
  <Status>Fail</Status>
  <ErrorCode>1019</ErrorCode>
  <ErrorMessage>Statement not ready.</ErrorMessage>
</FlexStatementResponse>
"""

GET_1018 = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse>
  <Status>Fail</Status>
  <ErrorCode>1018</ErrorCode>
  <ErrorMessage>Too many requests have been made from this token.</ErrorMessage>
</FlexStatementResponse>
"""

REPORT_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Test">
  <CashReport accountId="U1" currency="USD" endingCash="2000" />
  <Trades>
    <Trade accountId="U1" tradeDate="20250102" dateTime="20250102;120000" symbol="AAPL" buySell="BUY" quantity="10" netCash="-1000" transactionID="T1" tradeID="TR1" currency="USD" />
    <Trade accountId="U1" tradeDate="20250103" dateTime="20250103;120000" symbol="AAPL" buySell="SELL" quantity="-5" netCash="600" transactionID="T2" tradeID="TR2" currency="USD" />
    <Trade accountId="U1" tradeDate="20250103" levelOfDetail="CLOSED_LOT" symbol="AAPL" quantity="-5" costBasis="500" fifoPnlRealized="100" openDateTime="20240101;000000" tradeID="TR2" transactionID="T2" currency="USD" />
    <Trade accountId="U1" tradeDate="20250104" levelOfDetail="WASH_SALE" symbol="AAPL" quantity="-1" fifoPnlRealized="-10" holdingPeriodDateTime="20250104;000000" whenRealized="20250104;000000" whenReopened="20250105;000000" transactionID="W1" tradeID="WTR1" currency="USD" />
  </Trades>
  <CashTransactions>
    <CashTransaction accountId="U1" dateTime="20250105;120000" amount="5" type="Dividends" symbol="AAPL" levelOfDetail="DETAIL" transactionID="C1" currency="USD" balance="2000"/>
    <CashTransaction accountId="U1" dateTime="20250105;120000" amount="-1" type="Withholding Tax" symbol="AAPL" levelOfDetail="DETAIL" transactionID="C2" currency="USD"/>
    <CashTransaction accountId="U1" dateTime="20250106;120000" amount="-100" type="Deposits/Withdrawals" levelOfDetail="DETAIL" transactionID="C3" currency="USD" description="DISBURSEMENT"/>
  </CashTransactions>
  <OpenPositions>
    <OpenPosition accountId="U1" symbol="AAPL" position="5" marketValue="1100" costBasis="500"/>
  </OpenPositions>
</FlexQueryResponse>
"""

REPORT_XML_OUT_OF_RANGE = b"""<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="OutOfRange">
  <FlexStatements count="1">
    <FlexStatement accountId="U1" fromDate="20260101" toDate="20260101" period="LastBusinessDay" whenGenerated="20260102;000000">
      <OpenPositions>
        <OpenPosition accountId="U1" symbol="AAPL" position="5" marketValue="1100" reportDate="20260101"/>
      </OpenPositions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


class _Resp:
    def __init__(self, content: bytes):
        self.content = content
        self.status_code = 200
        self.content_type = "application/xml"


def _patch_http(monkeypatch):
    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl

    state = {"get_calls": 0}

    # Avoid real sleeping and avoid monotonic-based waits from the in-process limiter.
    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.GetUserInfo" in url:
            return _Resp(GET_USER_INFO)
        if "FlexStatementService.SendRequest" in url:
            return _Resp(SEND_OK)
        if "FlexStatementService.GetStatement" in url:
            state["get_calls"] += 1
            if state["get_calls"] == 1:
                return _Resp(NOT_READY)
            return _Resp(REPORT_XML)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)


def test_ib_flex_web_falls_back_on_404_base_url(session, monkeypatch):
    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl
    from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter

    state = {"send_calls": 0, "get_calls": 0}
    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.GetUserInfo" in url:
            return _Resp(GET_USER_INFO)
        if "FlexStatementService.SendRequest" in url:
            state["send_calls"] += 1
            # First base returns 404 (simulate wrong host/path), second succeeds.
            if state["send_calls"] == 1:
                raise mod.ProviderError("HTTP error status=404 host=ndcdyn.interactivebrokers.com path=/Universal/servlet/FlexStatementService.SendRequest")
            return _Resp(SEND_OK)
        if "FlexStatementService.GetStatement" in url:
            state["get_calls"] += 1
            if state["get_calls"] == 1:
                return _Resp(NOT_READY)
            return _Resp(REPORT_XML)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()

    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={"IB_FLEX_TOKEN": "TOK", "IB_FLEX_QUERY_ID": "QID"},
        run_settings={"effective_start_date": "2025-01-01", "effective_end_date": "2025-01-10"},
    )
    adapter = IBFlexWebAdapter()
    items, next_cursor = adapter.fetch_transactions(ctx, dt.date(2025, 1, 1), dt.date(2025, 1, 10), cursor=None)
    assert next_cursor is None
    assert any((it.get("record_kind") or "").upper() == "REPORT_PAYLOAD" for it in items)
    assert state["send_calls"] >= 2


def test_ib_flex_web_fetches_one_query_per_cursor(session, monkeypatch):
    import urllib.parse

    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl
    from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter

    state = {"send_calls": 0, "get_calls": 0, "last_query": None, "ready": {}}
    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    report_a = REPORT_XML.replace(b'queryName="Test"', b'queryName="A"')
    report_b = REPORT_XML.replace(b'queryName="Test"', b'queryName="B"').replace(b'transactionID="C3"', b'transactionID="C3B"')

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.GetUserInfo" in url:
            return _Resp(GET_USER_INFO)
        if "FlexStatementService.SendRequest" in url:
            state["send_calls"] += 1
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("q", [""])[0]
            state["last_query"] = q
            # Unique ref per query id.
            return _Resp(
                SEND_OK.replace(b"REF123", f"REF_{q}".encode("utf-8") if q else b"REF_UNKNOWN")
            )
        if "FlexStatementService.GetStatement" in url:
            state["get_calls"] += 1
            ref = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("q", [""])[0]
            # First call per ref returns not-ready, second returns payload.
            cnt = state["ready"].get(ref, 0) + 1
            state["ready"][ref] = cnt
            if cnt == 1:
                return _Resp(NOT_READY)
            if ref.endswith("111"):
                return _Resp(report_a)
            return _Resp(report_b)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"extra_query_ids": ["222"]},
    )
    session.add(conn)
    session.flush()

    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={"IB_FLEX_TOKEN": "TOK", "IB_FLEX_QUERY_ID": "111"},
        run_settings={"effective_start_date": "2025-01-01", "effective_end_date": "2025-01-10"},
    )
    adapter = IBFlexWebAdapter()

    # First page should submit only ONE SendRequest.
    items1, cur1 = adapter.fetch_transactions(ctx, dt.date(2025, 1, 1), dt.date(2025, 1, 10), cursor=None)
    assert cur1 == "1"
    assert state["send_calls"] == 1
    assert any((it.get("record_kind") or "").upper() == "REPORT_PAYLOAD" for it in items1)

    # Second page should submit the second query.
    items2, cur2 = adapter.fetch_transactions(ctx, dt.date(2025, 1, 1), dt.date(2025, 1, 10), cursor=cur1)
    assert cur2 is None
    assert state["send_calls"] == 2
    assert any((it.get("record_kind") or "").upper() == "REPORT_PAYLOAD" for it in items2)


def test_ib_flex_web_adapter_parses_trades_cashflows_and_broker_rows(session, monkeypatch):
    _patch_http(monkeypatch)

    from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()

    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={"IB_FLEX_TOKEN": "TOK", "IB_FLEX_QUERY_ID": "QID"},
        run_settings={"effective_start_date": "2025-01-01", "effective_end_date": "2025-01-10"},
    )

    adapter = IBFlexWebAdapter()
    accts = adapter.fetch_accounts(ctx)
    assert any(a.get("provider_account_id") == "IBFLEX:U1" for a in accts)

    items, next_cursor = adapter.fetch_transactions(ctx, dt.date(2025, 1, 1), dt.date(2025, 1, 10), cursor=None)
    assert next_cursor is None
    assert any((it.get("record_kind") or "").upper() == "REPORT_PAYLOAD" for it in items)
    tx_types = {it.get("type") for it in items if (it.get("record_kind") or "").upper() not in {"REPORT_PAYLOAD", "BROKER_CLOSED_LOT", "BROKER_WASH_SALE"}}
    assert "BUY" in tx_types
    assert "SELL" in tx_types
    assert "DIV" in tx_types
    assert "WITHHOLDING" in tx_types
    assert "TRANSFER" in tx_types
    assert any((it.get("record_kind") or "").upper() == "BROKER_CLOSED_LOT" for it in items)
    assert any((it.get("record_kind") or "").upper() == "BROKER_WASH_SALE" for it in items)
    # Cash balance record should be emitted when the report provides a balance field.
    assert any((it.get("record_kind") or "").upper() == "CASH_BALANCE" for it in items)

def test_ib_flex_web_fetch_holdings_uses_report_end_date_as_asof(session, monkeypatch):
    _patch_http(monkeypatch)

    from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()

    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={"IB_FLEX_TOKEN": "TOK", "IB_FLEX_QUERY_ID": "QID"},
        run_settings={"effective_start_date": "2025-01-01", "effective_end_date": "2025-01-10"},
    )
    adapter = IBFlexWebAdapter()

    # Populate holdings cache via transaction parsing.
    adapter.fetch_transactions(ctx, dt.date(2024, 12, 31), dt.date(2024, 12, 31), cursor=None)

    out = adapter.fetch_holdings(ctx, as_of=dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc))
    # Should prefer the report end date (12/31) rather than caller-provided timestamp.
    assert str(out.get("as_of") or "").startswith("2024-12-31T23:59:59")
    # Cash balance should be surfaced as CASH:USD when available.
    assert any(str(it.get("symbol") or "").upper() == "CASH:USD" for it in (out.get("items") or []))

def test_ib_flex_web_skips_holdings_when_report_dates_are_after_requested_end(session, monkeypatch):
    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl
    from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter
    from src.importers.adapters import ProviderError

    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    state = {"get_calls": 0}

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.GetUserInfo" in url:
            return _Resp(GET_USER_INFO)
        if "FlexStatementService.SendRequest" in url:
            return _Resp(SEND_OK)
        if "FlexStatementService.GetStatement" in url:
            state["get_calls"] += 1
            if state["get_calls"] == 1:
                return _Resp(NOT_READY)
            return _Resp(REPORT_XML_OUT_OF_RANGE)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()

    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={"IB_FLEX_TOKEN": "TOK", "IB_FLEX_QUERY_ID": "QID"},
        run_settings={"effective_start_date": "2024-12-31", "effective_end_date": "2024-12-31"},
    )
    adapter = IBFlexWebAdapter()

    adapter.fetch_transactions(ctx, dt.date(2024, 12, 31), dt.date(2024, 12, 31), cursor=None)
    with pytest.raises(ProviderError):
        adapter.fetch_holdings(ctx, as_of=dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc))
    assert any("ignored requested end date" in str(w).lower() for w in (ctx.run_settings or {}).get("adapter_warnings", []))


def test_ib_flex_web_sync_idempotent_payload_skip(session, monkeypatch):
    _patch_http(monkeypatch)
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret-key-32-bytes-minimum!!")

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN", plaintext="TOK")
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID", plaintext="QID")
    session.commit()

    # First run imports.
    r1 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 10),
        actor="test",
        reprocess_files=False,
    )
    assert r1.status == "SUCCESS"
    assert session.query(Transaction).count() > 0
    assert session.query(ExternalTransactionMap).count() > 0
    assert session.query(BrokerLotClosure).count() == 1
    assert session.query(BrokerWashSaleEvent).count() == 1
    assert session.query(ExternalHoldingSnapshot).count() == 1

    tx_count = session.query(Transaction).count()
    map_count = session.query(ExternalTransactionMap).count()
    snap_count = session.query(ExternalHoldingSnapshot).count()

    # Second run re-fetches identical payload -> should not create new transactions or holdings snapshots.
    # Reset stub call counters by re-patching.
    _patch_http(monkeypatch)
    r2 = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 10),
        actor="test",
        reprocess_files=False,
    )
    assert r2.status == "SUCCESS"
    assert session.query(Transaction).count() == tx_count
    assert session.query(ExternalTransactionMap).count() == map_count
    assert session.query(ExternalHoldingSnapshot).count() == snap_count
    cov = r2.coverage_json or {}
    assert int(cov.get("report_payloads_skipped") or 0) >= 1


def test_ib_flex_web_retries_on_1018_send_request_then_succeeds(session, monkeypatch):
    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl

    sleeps: list[float] = []
    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(float(s)))

    state = {"send_calls": 0, "get_calls": 0}

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.SendRequest" in url:
            state["send_calls"] += 1
            if state["send_calls"] == 1:
                return _Resp(SEND_1018)
            return _Resp(SEND_OK)
        if "FlexStatementService.GetStatement" in url:
            state["get_calls"] += 1
            return _Resp(REPORT_XML)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret-key-32-bytes-minimum!!")

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN", plaintext="TOK")
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID", plaintext="123")
    session.commit()

    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 10),
        actor="test",
    )
    assert run.status == "SUCCESS"
    cov = run.coverage_json or {}
    qa = cov.get("ib_flex_web_query_audit") or []
    assert qa and isinstance(qa, list)
    assert any((r or {}).get("status") == "SUCCESS" for r in qa if isinstance(r, dict))
    assert any(float(s) >= 5.0 for s in sleeps)


def test_ib_flex_web_retries_on_1018_get_statement_then_succeeds(session, monkeypatch):
    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl

    sleeps: list[float] = []
    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(float(s)))

    state = {"get_calls": 0}

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.SendRequest" in url:
            return _Resp(SEND_OK)
        if "FlexStatementService.GetStatement" in url:
            state["get_calls"] += 1
            if state["get_calls"] == 1:
                return _Resp(GET_1018)
            return _Resp(REPORT_XML)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret-key-32-bytes-minimum!!")

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN", plaintext="TOK")
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID", plaintext="123")
    session.commit()

    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 10),
        actor="test",
    )
    assert run.status == "SUCCESS"
    cov = run.coverage_json or {}
    qa = cov.get("ib_flex_web_query_audit") or []
    assert any((r or {}).get("poll_retries", 0) >= 1 for r in qa if isinstance(r, dict))
    assert any(float(s) >= 5.0 for s in sleeps)


def test_ib_flex_web_rate_limit_exceeded_skips_one_query_but_run_succeeds(session, monkeypatch):
    import urllib.parse

    import src.adapters.ib_flex_web.adapter as mod
    import src.utils.rate_limit as rl

    mono = {"t": 0.0}

    def fake_mono():
        mono["t"] += 100.0
        return mono["t"]

    monkeypatch.setattr(rl.time, "monotonic", fake_mono)
    monkeypatch.setattr(rl.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    def fake_http_get(url: str, *args, **kwargs):
        if "FlexStatementService.SendRequest" in url:
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("q", [""])[0]
            if q == "111":
                return _Resp(SEND_1018)
            return _Resp(SEND_OK.replace(b"REF123", f"REF_{q}".encode("utf-8")))
        if "FlexStatementService.GetStatement" in url:
            return _Resp(REPORT_XML)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "http_get", fake_http_get)
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret-key-32-bytes-minimum!!")

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"extra_query_ids": ["222"]},
    )
    session.add(conn)
    session.flush()
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN", plaintext="TOK")
    upsert_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID", plaintext="111")
    session.commit()

    run = run_sync(
        session,
        connection_id=conn.id,
        mode="FULL",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 10),
        actor="test",
    )
    # Second query should still import.
    assert run.status == "SUCCESS"
    cov = run.coverage_json or {}
    assert int(cov.get("txn_count") or 0) > 0
    warns = cov.get("warnings") or []
    assert any("rate limit (1018)" in str(w).lower() for w in warns)
