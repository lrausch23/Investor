import datetime as dt

from src.investor.cash_bills.selectors import compute_summary, coverage_status, derive_status, filter_bills


def _bill(
    *,
    due: str,
    balance: float,
    status: str = "upcoming",
):
    return {
        "id": "x",
        "card_name": "Test Card",
        "issuer": "Test",
        "last4": "0000",
        "due_date": due,
        "statement_balance": balance,
        "minimum_due": None,
        "autopay": "unknown",
        "status": status,
    }


def test_derive_status_basic():
    as_of = dt.date(2026, 1, 15)
    assert derive_status(_bill(due="2026-01-10", balance=10), as_of) == "overdue"
    assert derive_status(_bill(due="2026-01-18", balance=10), as_of) == "due_soon"
    assert derive_status(_bill(due="2026-02-20", balance=10), as_of) == "upcoming"
    assert derive_status(_bill(due="2026-01-20", balance=10, status="paid"), as_of) == "paid"


def test_filter_bills_range_and_status():
    as_of = dt.date(2026, 1, 15)
    bills = [
        _bill(due="2026-01-10", balance=10),  # overdue
        _bill(due="2026-01-18", balance=20),  # due soon
        _bill(due="2026-02-10", balance=30),  # upcoming
    ]
    filtered = filter_bills(bills, as_of=as_of, range_days=30, status_filter="all")
    assert len(filtered) == 3
    overdue_only = filter_bills(bills, as_of=as_of, range_days=7, status_filter="overdue")
    assert len(overdue_only) == 1
    due_soon_only = filter_bills(bills, as_of=as_of, range_days=7, status_filter="due_soon")
    assert len(due_soon_only) == 1


def test_compute_summary_and_coverage():
    as_of = dt.date(2026, 1, 15)
    bills = [
        _bill(due="2026-01-18", balance=100),
        _bill(due="2026-01-25", balance=200),
        _bill(due="2026-03-01", balance=300),
    ]
    cash = [
        {"id": "c1", "account_name": "Cash 1", "available_balance": 250.0, "current_balance": None, "last_updated": None},
        {"id": "c2", "account_name": "Cash 2", "available_balance": 50.0, "current_balance": None, "last_updated": None},
    ]
    summary = compute_summary(bills, cash, as_of=as_of, range_days=30)
    assert summary["cash_available_total"] == 300.0
    assert summary["cc_due_total_30d"] == 300.0
    assert summary["net_after_bills_30d"] == 0.0
    assert coverage_status(summary["cash_available_total"], summary["cc_due_total_30d"]) == "tight"
