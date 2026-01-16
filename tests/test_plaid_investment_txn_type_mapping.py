from __future__ import annotations

from src.adapters.plaid_chase.adapter import _map_plaid_investment_txn_type


def test_plaid_investment_cash_dividend_not_misclassified_as_fee() -> None:
    raw = {
        "type": "fee",
        "subtype": "cash dividend",
        "name": "NVIDIA CORP CASH DIV ON 3000 SHS REC 12/05/24 PAY 12/27/24",
    }
    assert _map_plaid_investment_txn_type(raw) == "DIV"


def test_plaid_investment_transfer_sign_normalization() -> None:
    from src.adapters.plaid_chase.adapter import _signed_amount_for_type

    # Plaid convention often uses positive = cash outflow. Investor uses negative for outflow.
    assert _signed_amount_for_type("TRANSFER", 20000.0) == -20000.0
    # Inflow should be positive.
    assert _signed_amount_for_type("TRANSFER", -150.0) == 150.0


def test_plaid_investment_withholding_detected_from_name_hint() -> None:
    from src.adapters.plaid_chase.adapter import _map_plaid_investment_txn_type

    raw = {
        "type": "fee",
        "subtype": "",
        "name": "TAIWAN SEMICONDUCTOR ... FOREIGN TAX WITHHELD",
    }
    assert _map_plaid_investment_txn_type(raw) == "WITHHOLDING"
