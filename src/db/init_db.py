from __future__ import annotations

import datetime as dt

from src.db.models import Base
from src.db.session import get_engine
from src.db.session import get_session
from src.db.models import TaxAssumptionsSet
from src.db.sqlite_migrations import ensure_sqlite_schema


def init_db() -> None:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    ensure_sqlite_schema(engine)
    # Post-create bootstrapping (keeps read-only pages from needing to write).
    with get_session() as session:
        existing = session.query(TaxAssumptionsSet).filter(TaxAssumptionsSet.name == "Default").one_or_none()
        if existing is None:
            session.add(
                TaxAssumptionsSet(
                    name="Default",
                    effective_date=dt.date.today(),
                    json_definition={
                        "ordinary_rate": 0.37,
                        "ltcg_rate": 0.20,
                        "state_rate": 0.05,
                        "niit_enabled": True,
                        "niit_rate": 0.038,
                        "qualified_dividend_pct": 0.0,
                    },
                )
            )
            session.commit()
