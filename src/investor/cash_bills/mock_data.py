from __future__ import annotations

import datetime as dt

from src.investor.cash_bills.types import CashBillsDashboard


def mock_cash_bills_data(as_of: dt.date | None = None) -> CashBillsDashboard:
    today = as_of or dt.date.today()

    def d(days: int) -> str:
        return (today + dt.timedelta(days=days)).isoformat()

    bills = [
        {
            "id": "bill_1",
            "card_name": "Sapphire Preferred",
            "issuer": "Chase",
            "last4": "4477",
            "due_date": d(5),
            "current_balance": 1380.22,
            "statement_balance": 1250.34,
            "minimum_due": 35.00,
            "autopay": "on",
            "status": "upcoming",
        },
        {
            "id": "bill_2",
            "card_name": "Prime Visa",
            "issuer": "Chase",
            "last4": "4549",
            "due_date": d(16),
            "current_balance": 712.01,
            "statement_balance": 642.18,
            "minimum_due": 25.00,
            "autopay": "off",
            "status": "upcoming",
        },
        {
            "id": "bill_3",
            "card_name": "Blue Cash Preferred",
            "issuer": "AMEX",
            "last4": "3000",
            "due_date": d(-3),
            "current_balance": 980.55,
            "statement_balance": 890.12,
            "minimum_due": 40.00,
            "autopay": "unknown",
            "status": "overdue",
        },
        {
            "id": "bill_4",
            "card_name": "Apple Card",
            "issuer": "GS",
            "last4": "0000",
            "due_date": d(42),
            "current_balance": 2245.70,
            "statement_balance": 2130.55,
            "minimum_due": None,
            "autopay": "on",
            "status": "upcoming",
        },
    ]

    cash_accounts = [
        {
            "id": "cash_1",
            "account_name": "Chase Checking ****5896",
            "available_balance": 18650.12,
            "current_balance": 19050.12,
            "last_updated": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)).isoformat(),
        },
        {
            "id": "cash_2",
            "account_name": "CPC Checking ****0787",
            "available_balance": 6240.88,
            "current_balance": 6240.88,
            "last_updated": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=5)).isoformat(),
        },
        {
            "id": "cash_3",
            "account_name": "Business Checking ****7638",
            "available_balance": 12500.00,
            "current_balance": 12620.33,
            "last_updated": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat(),
        },
    ]

    return {
        "as_of": today.isoformat(),
        "bills": bills,
        "cash_accounts": cash_accounts,
    }
