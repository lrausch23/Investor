from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db.models import ExpenseAccount, ExpenseCategory, ExpenseImportBatch, ExpenseRule, ExpenseTransaction


def cleanup_orphan_import_batches(*, session: Session) -> int:
    """
    Deletes import batches that are no longer referenced by any expense transactions.
    Returns number of batches deleted.
    """
    rows = session.execute(
        text(
            """
            DELETE FROM expense_import_batches
            WHERE id NOT IN (SELECT DISTINCT import_batch_id FROM expense_transactions)
            """
        )
    )
    return int(getattr(rows, "rowcount", 0) or 0)


def purge_account_data(*, session: Session, account_id: int) -> dict[str, int]:
    """
    Purge data for a single expense account:
    - deletes expense_transactions for that account
    - deletes the expense_account row
    - cleans up orphan import batches
    """
    tx_deleted = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.expense_account_id == int(account_id))
        .delete(synchronize_session=False)
    )
    acct_deleted = session.query(ExpenseAccount).filter(ExpenseAccount.id == int(account_id)).delete(synchronize_session=False)
    batch_deleted = cleanup_orphan_import_batches(session=session)
    return {"transactions_deleted": int(tx_deleted or 0), "accounts_deleted": int(acct_deleted or 0), "batches_deleted": int(batch_deleted or 0)}


def purge_all_expenses_data(
    *,
    session: Session,
    include_rules: bool = False,
    include_categories: bool = False,
) -> dict[str, int]:
    """
    Purge all expense data:
    - deletes all expense_transactions, expense_accounts, expense_import_batches
    - optionally deletes learned expense_rules and user expense_categories
    """
    tx_deleted = session.query(ExpenseTransaction).delete(synchronize_session=False)
    acct_deleted = session.query(ExpenseAccount).delete(synchronize_session=False)
    batch_deleted = session.query(ExpenseImportBatch).delete(synchronize_session=False)
    rules_deleted = 0
    categories_deleted = 0
    if include_rules:
        rules_deleted = session.query(ExpenseRule).delete(synchronize_session=False)
    if include_categories:
        categories_deleted = session.query(ExpenseCategory).delete(synchronize_session=False)
    return {
        "transactions_deleted": int(tx_deleted or 0),
        "accounts_deleted": int(acct_deleted or 0),
        "batches_deleted": int(batch_deleted or 0),
        "rules_deleted": int(rules_deleted or 0),
        "categories_deleted": int(categories_deleted or 0),
    }

