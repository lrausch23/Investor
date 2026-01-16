from src.adapters.plaid_chase.adapter import PlaidChaseAdapter


class PlaidAmexAdapter(PlaidChaseAdapter):
    """
    Plaid-backed American Express connector intended for automated credit card expense ingestion.

    Connection:
      - provider=PLAID
      - broker=AMEX
      - connector=AMEX_PLAID
    """

