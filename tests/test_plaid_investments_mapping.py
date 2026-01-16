from __future__ import annotations

from src.core.sync_runner import _plaid_account_type_for_investment, _plaid_is_investment_account


def test_plaid_is_investment_account() -> None:
    assert _plaid_is_investment_account({"raw_type": "investment"}) is True
    assert _plaid_is_investment_account({"raw_type": "Investment"}) is True
    assert _plaid_is_investment_account({"raw_type": "depository"}) is False
    assert _plaid_is_investment_account({"raw_type": ""}) is False
    assert _plaid_is_investment_account({}) is False


def test_plaid_investment_account_type_mapping() -> None:
    assert _plaid_account_type_for_investment({"raw_subtype": "ira"}) == "IRA"
    assert _plaid_account_type_for_investment({"raw_subtype": "roth_ira"}) == "IRA"
    assert _plaid_account_type_for_investment({"raw_subtype": "401k"}) == "IRA"
    assert _plaid_account_type_for_investment({"raw_subtype": "taxable"}) == "TAXABLE"
    assert _plaid_account_type_for_investment({"raw_subtype": ""}) == "TAXABLE"
    assert _plaid_account_type_for_investment({}) == "TAXABLE"

