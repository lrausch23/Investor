from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.db.models import AuditLog
from src.utils.time import utcnow


def log_change(
    session: Session,
    *,
    actor: str,
    action: str,
    entity: str,
    entity_id: Optional[str],
    old: Optional[dict[str, Any]],
    new: Optional[dict[str, Any]],
    note: Optional[str] = None,
) -> None:
    session.add(
        AuditLog(
            at=utcnow(),
            actor=actor,
            action=action,
            entity=entity,
            entity_id=entity_id,
            old_json=old,
            new_json=new,
            note=note,
        )
    )
