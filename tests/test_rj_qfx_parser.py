from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.adapters.rj_offline.qfx_parser import (
    extract_qfx_header_meta,
    parse_positions,
    parse_security_list,
    parse_transactions,
    stable_txn_id_from_qfx,
)


def test_qfx_parses_header_meta():
    txt = Path("tests/fixtures/rj_qfx_minimal.qfx").read_text(encoding="utf-8-sig", errors="ignore")
    meta = extract_qfx_header_meta(txt)
    assert meta.broker_id == "RJ"
    assert meta.acct_id == "xxxxW554"
    assert meta.dt_start == dt.date(2025, 1, 1)
    assert meta.dt_end == dt.date(2025, 1, 31)
    assert meta.dt_asof == dt.date(2026, 1, 7)


def test_qfx_parses_securities_positions_and_transactions():
    txt = Path("tests/fixtures/rj_qfx_minimal.qfx").read_text(encoding="utf-8-sig", errors="ignore")
    sec = parse_security_list(txt)
    assert "037833100" in sec
    assert sec["037833100"].ticker == "AAPL"

    asof, pos, meta = parse_positions(txt, securities=sec)
    assert asof == dt.date(2026, 1, 7)
    assert len(pos) == 1
    assert pos[0].unique_id == "037833100"
    assert float(pos[0].qty or 0.0) == 10.0
    assert float(pos[0].market_value or 0.0) == 2000.0
    assert float(meta.get("avail_cash") or 0.0) == 100.0

    tx = parse_transactions(txt)
    assert len(tx) >= 2
    assert {t.fitid for t in tx if t.fitid} >= {"F1", "F2"}


def test_qfx_stable_txn_id_prefers_fitid():
    txt = Path("tests/fixtures/rj_qfx_minimal.qfx").read_text(encoding="utf-8-sig", errors="ignore")
    txs = parse_transactions(txt)
    provider_account_id = "RJ:TAXABLE"
    ids = [stable_txn_id_from_qfx(provider_account_id=provider_account_id, tx=t) for t in txs]
    assert any(x.startswith("RJ:FITID:F1") for x in ids)
    assert any(x.startswith("RJ:FITID:F2") for x in ids)


def test_qfx_parses_nested_invbuy_fields_used_by_rj():
    txt = """
OFXHEADER:100
<OFX>
  <INVSTMTMSGSRSV1>
    <INVSTMTTRNRS>
      <INVSTMTRS>
        <INVTRANLIST>
          <BUYSTOCK>
            <INVBUY>
              <INVTRAN>
                <FITID>BUY-GLW-1
                <DTTRADE>20260203120000.000[-5:EST]
                <DTSETTLE>20260204120000.000[-5:EST]
                <MEMO>Purchased 500 shares @ $115.45 CORNING INCORPORATED
              </INVTRAN>
              <SECID>
                <UNIQUEID>219350105
                <UNIQUEIDTYPE>CUSIP
              </SECID>
              <UNITS>500
              <UNITPRICE>115.45
              <COMMISSION>0.00
              <FEES>0.00
              <TOTAL>-57725.00
            </INVBUY>
            <BUYTYPE>BUY
          </BUYSTOCK>
        </INVTRANLIST>
      </INVSTMTRS>
    </INVSTMTTRNRS>
  </INVSTMTMSGSRSV1>
</OFX>
"""
    txs = parse_transactions(txt)

    assert len(txs) == 1
    tx = txs[0]
    assert tx.fitid == "BUY-GLW-1"
    assert tx.dt_trade == dt.date(2026, 2, 3)
    assert tx.dt_posted == dt.date(2026, 2, 4)
    assert tx.raw_type == "BUYSTOCK"
    assert tx.unique_id == "219350105"
    assert tx.units == 500.0
    assert tx.unit_price == 115.45
    assert tx.commission == 0.0
    assert tx.fees == 0.0
    assert tx.amount == -57725.0
