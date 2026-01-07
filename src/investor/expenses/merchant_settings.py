from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from src.db.models import ExpenseMerchantSetting


def merchant_key(value: str) -> str:
    return " ".join((value or "").strip().split()).casefold()


@dataclass(frozen=True)
class MerchantRecurringSetting:
    merchant_display: str
    recurring_enabled: bool
    cadence: str


def get_merchant_setting(session: Session, *, merchant: str) -> MerchantRecurringSetting | None:
    key = merchant_key(merchant)
    if not key:
        return None
    row = session.query(ExpenseMerchantSetting).filter(ExpenseMerchantSetting.merchant_key == key).one_or_none()
    if row is None:
        return None
    return MerchantRecurringSetting(
        merchant_display=row.merchant_display,
        recurring_enabled=bool(row.recurring_enabled),
        cadence=str(row.cadence or "UNKNOWN"),
    )


def upsert_merchant_setting(
    session: Session,
    *,
    merchant: str,
    recurring_enabled: bool,
    cadence: str,
) -> None:
    key = merchant_key(merchant)
    display = " ".join((merchant or "").strip().split())
    if not key or not display:
        raise ValueError("Merchant is required")
    cad = (cadence or "UNKNOWN").strip().upper()
    allowed = {"WEEKLY", "MONTHLY", "QUARTERLY", "SEMIANNUAL", "ANNUAL", "UNKNOWN"}
    if cad not in allowed:
        raise ValueError(f"Invalid cadence: {cad}")

    row = session.query(ExpenseMerchantSetting).filter(ExpenseMerchantSetting.merchant_key == key).one_or_none()
    if row is None:
        row = ExpenseMerchantSetting(merchant_key=key, merchant_display=display)
        session.add(row)
        session.flush()
    row.merchant_display = display
    row.recurring_enabled = bool(recurring_enabled)
    row.cadence = cad
    # updated_at is set by app logic (UTCDateTime default); update explicitly on write.
    from src.utils.time import utcnow

    row.updated_at = utcnow()

