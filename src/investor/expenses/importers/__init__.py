from __future__ import annotations

from src.investor.expenses.importers.base import StatementImporter
from src.investor.expenses.importers.chase_card_csv import ChaseCardCSVImporter
from src.investor.expenses.importers.chase_bank_csv import ChaseBankCSVImporter
from src.investor.expenses.importers.amex_csv import AmexCSVImporter
from src.investor.expenses.importers.apple_card_csv import AppleCardCSVImporter
from src.investor.expenses.importers.generic_bank_csv import GenericBankCSVImporter


def default_importers() -> list[StatementImporter]:
    return [ChaseCardCSVImporter(), ChaseBankCSVImporter(), AmexCSVImporter(), AppleCardCSVImporter(), GenericBankCSVImporter()]
