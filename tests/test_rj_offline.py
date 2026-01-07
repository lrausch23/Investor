from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace

from src.adapters.rj_offline.adapter import (
    RJOfflineAdapter,
    _looks_like_holdings,
    _looks_like_realized_pl,
    _looks_like_transactions,
)


def test_rj_offline_header_detection() -> None:
    holdings = "Symbol,Quantity,Market Value\nAAPL,10,2000\n"
    txns = "Date,Description,Amount,Symbol,Quantity\n2025-01-02,BUY AAPL,-2000,AAPL,10\n"
    txns_symbol_cusip_tsv = (
        "Account\tDate\tCategory\tType\tSymbol/CUSIP\tDescription\tQuantity\tPrice\tAmount\tAdditional Detail\n"
        "Kolozsi LLC xxxxW554\t12/16/2025\tExpense\tTaxes Withheld\tUNH\tUNITEDHEALTH GROUP INCORPORATED\t0.00000\t$0.00\t($331.50)\tForeign taxes withheld\n"
    )
    positions_current_value_tsv = (
        "Description\tSYMBOL/CUSIP\tQuantity\tDelayed Price\tCurrent Value\tDaily Price Change\tDaily Value Change\tInvestment Gain/(Loss)\tAmount Invested / Unit\tProduct Type\tAmount Invested (†)\tEstimated Annual Income\tTime Held\n"
        "Raymond James Bank Deposit Program\t\t187,869.680\t$1.00*\t$187,869.68\t\t\t\t$1.00\tCash & Cash Alternatives\t$187,869.68\t$56.36\t\n"
    )
    assert _looks_like_holdings(holdings)
    assert not _looks_like_transactions(holdings)
    assert _looks_like_transactions(txns)
    assert not _looks_like_holdings(txns)
    assert _looks_like_transactions(txns_symbol_cusip_tsv)
    assert _looks_like_holdings(positions_current_value_tsv)

    realized_pl = (
        "Description (Symbol/CUSIP)\tQuantity\tOpening Date\tOpening Amount\tClosing Date\tClosing Amount\tTime Held\tRealized Gain/(Loss)$\tRealized Gain/(Loss)%\n"
        "ADVANCED MICRO DEVICES INCORPORATED (AMD)\t400.000\t11/20/2025\t$93,144.00\t12/15/2025\t$84,228.04\tShort\t($8,915.96)\t-9.57%\n"
    )
    assert _looks_like_realized_pl(realized_pl)


def test_rj_offline_parses_holdings_and_transactions(tmp_path: Path) -> None:
    holdings_path = tmp_path / "rj_holdings.csv"
    holdings_path.write_text("Symbol,Quantity,Market Value,Cost Basis\nAAPL,10,2000,1500\n", encoding="utf-8")
    tx_path = tmp_path / "rj_activity.csv"
    tx_path.write_text("Date,Description,Amount,Symbol,Quantity\n2025-01-02,BUY AAPL,-2000,AAPL,10\n", encoding="utf-8")

    adapter = RJOfflineAdapter()
    conn = SimpleNamespace(
        id=123,
        metadata_json={"data_dir": str(tmp_path)},
        run_settings={
            "selected_files": [
                {"path": str(tx_path), "file_hash": "x", "kind": "TRANSACTIONS", "file_name": tx_path.name}
            ]
        },
    )

    holdings = adapter.fetch_holdings(conn, as_of=None)
    assert isinstance(holdings, dict)
    assert len(list(holdings.get("items") or [])) == 1
    assert holdings["items"][0]["symbol"] == "AAPL"
    assert holdings["items"][0]["provider_account_id"] == "RJ:TAXABLE"

    txns, next_cursor = adapter.fetch_transactions(conn, dt.date(2025, 1, 1), dt.date(2025, 12, 31), cursor=None)
    assert next_cursor is None
    assert len(txns) == 1
    assert txns[0]["ticker"] == "AAPL"
    assert txns[0]["provider_account_id"] == "RJ:TAXABLE"


def test_rj_offline_parses_symbol_cusip_activity_row(tmp_path: Path) -> None:
    tx_path = tmp_path / "activity.tsv"
    tx_path.write_text(
        "Account\tDate\tCategory\tType\tSymbol/CUSIP\tDescription\tQuantity\tPrice\tAmount\tAdditional Detail\n"
        "Kolozsi LLC xxxxW554\t12/16/2025\tExpense\tTaxes Withheld\tUNH\tUNITEDHEALTH GROUP INCORPORATED\t0.00000\t$0.00\t($331.50)\tForeign taxes withheld\n",
        encoding="utf-8",
    )
    adapter = RJOfflineAdapter()
    conn = SimpleNamespace(
        id=123,
        metadata_json={"data_dir": str(tmp_path)},
        run_settings={"selected_files": [{"path": str(tx_path), "file_hash": "x", "kind": "TRANSACTIONS"}]},
    )
    txns, next_cursor = adapter.fetch_transactions(conn, dt.date(2025, 1, 1), dt.date(2025, 12, 31), cursor=None)
    assert next_cursor is None
    assert len(txns) == 1
    assert txns[0]["ticker"] == "UNH"
    assert txns[0]["type"] == "WITHHOLDING"
    assert abs(float(txns[0]["amount"]) - (-331.50)) <= 1e-6


def test_rj_offline_parses_positions_cash_row_as_cash_balance(tmp_path: Path) -> None:
    holdings_path = tmp_path / "positions.tsv"
    holdings_path.write_text(
        "Description\tSYMBOL/CUSIP\tQuantity\tDelayed Price\tCurrent Value\tDaily Price Change\tDaily Value Change\tInvestment Gain/(Loss)\tAmount Invested / Unit\tProduct Type\tAmount Invested (†)\tEstimated Annual Income\tTime Held\n"
        "Raymond James Bank Deposit Program\t\t187,869.680\t$1.00*\t$187,869.68\t\t\t\t$1.00\tCash & Cash Alternatives\t$187,869.68\t$56.36\t\n",
        encoding="utf-8",
    )
    adapter = RJOfflineAdapter()
    conn = SimpleNamespace(id=123, metadata_json={"data_dir": str(tmp_path)}, run_settings={})
    holdings = adapter.fetch_holdings(conn, as_of=None)
    assert isinstance(holdings, dict)
    assert "cash_balances" in holdings
    assert len(list(holdings.get("cash_balances") or [])) == 1
    cb = holdings["cash_balances"][0]
    assert cb["provider_account_id"] == "RJ:TAXABLE"
    assert abs(float(cb["amount"]) - 187_869.68) <= 1e-6
    # Also emits a CASH item for fallback display.
    cash_items = [it for it in (holdings.get("items") or []) if str(it.get("symbol") or "").startswith("CASH:")]
    assert cash_items and abs(float(cash_items[0]["market_value"]) - 187_869.68) <= 1e-6


def test_rj_offline_parses_realized_pl_as_broker_closed_lots(tmp_path: Path) -> None:
    p = tmp_path / "realized.tsv"
    p.write_text(
        "Description (Symbol/CUSIP)\tQuantity\tOpening Date\tOpening Amount\tClosing Date\tClosing Amount\tTime Held\tRealized Gain/(Loss)$\tRealized Gain/(Loss)%\n"
        "ADVANCED MICRO DEVICES INCORPORATED (AMD)\t400.000\t11/20/2025\t$93,144.00\t12/15/2025\t$84,228.04\tShort\t($8,915.96)\t-9.57%\n",
        encoding="utf-8",
    )
    adapter = RJOfflineAdapter()
    conn = SimpleNamespace(
        id=123,
        metadata_json={"data_dir": str(tmp_path)},
        run_settings={"selected_files": [{"path": str(p), "file_hash": "fh", "kind": "TRANSACTIONS"}]},
    )
    items, next_cursor = adapter.fetch_transactions(conn, dt.date(2025, 1, 1), dt.date(2025, 12, 31), cursor=None)
    assert next_cursor is None
    assert len(items) == 1
    it = items[0]
    assert it["record_kind"] == "BROKER_CLOSED_LOT"
    assert it["symbol"] == "AMD"
    assert it["date"] == "2025-12-15"
    assert abs(float(it["qty"]) - 400.0) <= 1e-9
    assert abs(float(it["cost_basis"]) - 93144.0) <= 1e-6
    assert abs(float(it["proceeds_derived"]) - 84228.04) <= 1e-6
    assert abs(float(it["realized_pl_fifo"]) - (-8915.96)) <= 1e-6
    assert it["source_file_hash"] == "fh"
