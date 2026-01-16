from __future__ import annotations

from src.adapters.plaid_chase.adapter import _map_plaid_investment_txn_type, _signed_amount_for_type


def test_plaid_investment_txn_type_mapping() -> None:
    assert _map_plaid_investment_txn_type({"type": "buy"}) == "BUY"
    assert _map_plaid_investment_txn_type({"type": "sell"}) == "SELL"
    assert _map_plaid_investment_txn_type({"type": "dividend"}) == "DIV"
    assert _map_plaid_investment_txn_type({"type": "interest"}) == "INT"
    assert _map_plaid_investment_txn_type({"type": "fee"}) == "FEE"
    assert _map_plaid_investment_txn_type({"type": "cash", "subtype": "transfer"}) == "TRANSFER"
    assert _map_plaid_investment_txn_type({"type": "unknown"}) == "OTHER"


def test_signed_amount_for_type() -> None:
    assert _signed_amount_for_type("BUY", 10.0) == -10.0
    assert _signed_amount_for_type("BUY", -10.0) == -10.0
    assert _signed_amount_for_type("SELL", 10.0) == 10.0
    assert _signed_amount_for_type("SELL", -10.0) == 10.0
    assert _signed_amount_for_type("DIV", -5.0) == 5.0
    assert _signed_amount_for_type("INT", 5.0) == 5.0
    assert _signed_amount_for_type("FEE", 2.0) == -2.0
    assert _signed_amount_for_type("WITHHOLDING", -1.0) == 1.0
    # Transfer: normalize to Investor (outflow negative, inflow positive).
    assert _signed_amount_for_type("TRANSFER", 7.0) == -7.0
    assert _signed_amount_for_type("TRANSFER", -7.0) == 7.0
