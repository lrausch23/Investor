from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy.orm import Session

from src.core.policy_engine import create_policy_version
from src.db.models import Account, BucketPolicy, TaxpayerEntity


def ensure_default_setup(session: Session, *, effective_date: dt.date) -> dict[str, Any]:
    created: dict[str, Any] = {"taxpayers": [], "accounts": [], "policy_id": None}

    trust = session.query(TaxpayerEntity).filter(TaxpayerEntity.name == "Trust").one_or_none()
    if trust is None:
        trust = TaxpayerEntity(name="Trust", type="TRUST", tax_id_last4=None, notes="Taxable trust scope")
        session.add(trust)
        session.flush()
        created["taxpayers"].append({"id": trust.id, "name": trust.name})

    personal = session.query(TaxpayerEntity).filter(TaxpayerEntity.name == "Personal").one_or_none()
    if personal is None:
        personal = TaxpayerEntity(name="Personal", type="PERSONAL", tax_id_last4=None, notes="Personal (IRA scope)")
        session.add(personal)
        session.flush()
        created["taxpayers"].append({"id": personal.id, "name": personal.name})

    def _ensure_account(name: str, broker: str, account_type: str, taxpayer_id: int) -> None:
        existing = session.query(Account).filter(Account.name == name).one_or_none()
        if existing is None:
            acct = Account(name=name, broker=broker, account_type=account_type, taxpayer_entity_id=taxpayer_id)
            session.add(acct)
            session.flush()
            created["accounts"].append({"id": acct.id, "name": acct.name})

    _ensure_account("IB Taxable", "IB", "TAXABLE", trust.id)
    _ensure_account("RJ Taxable", "RJ", "TAXABLE", trust.id)
    _ensure_account("Chase IRA", "CHASE", "IRA", personal.id)

    existing_policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    if existing_policy is None:
        policy = create_policy_version(
            session=session,
            name="Household Policy",
            effective_date=effective_date,
            json_definition={"notes": "Default MVP policy", "constraints": {"max_single_name_pct": 0.15}},
            buckets=[
                ("B1", "Liquidity", 0.05, 0.10, 0.20, ["CASH", "MMF"], {}),
                ("B2", "Defensive / Income", 0.20, 0.30, 0.45, ["BOND", "CREDIT", "DIVIDEND"], {}),
                ("B3", "Growth", 0.30, 0.45, 0.65, ["EQUITY", "INDEX", "GROWTH"], {}),
                ("B4", "Alpha / Opportunistic", 0.00, 0.15, 0.25, ["ALTERNATIVE", "THEMATIC", "ALPHA"], {}),
            ],
        )
        session.flush()
        created["policy_id"] = policy.id
    else:
        created["policy_id"] = existing_policy.id

    return created
