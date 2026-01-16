from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy.orm import Session

from src.db.models import TickerClassification
from src.investor.momentum.utils import normalize_ticker


@dataclass(frozen=True)
class ClassificationRow:
    ticker: str
    sector: str | None
    industry: str | None
    as_of_date: dt.date | None
    source: str


class ClassificationService:
    def get_map(self, session: Session, tickers: Iterable[str]) -> dict[str, ClassificationRow]:
        ts = [normalize_ticker(t) for t in tickers]
        ts = [t for t in ts if t]
        ts = list(dict.fromkeys(ts))
        if not ts:
            return {}
        rows = session.query(TickerClassification).filter(TickerClassification.ticker.in_(ts)).all()
        out: dict[str, ClassificationRow] = {}
        for r in rows:
            t = normalize_ticker(r.ticker)
            if not t:
                continue
            out[t] = ClassificationRow(
                ticker=t,
                sector=(r.sector or None),
                industry=(r.industry or None),
                as_of_date=(r.as_of_date if isinstance(r.as_of_date, dt.date) else None),
                source=str(r.source or "manual"),
            )
        return out

    def upsert_many(
        self,
        session: Session,
        *,
        rows: list[ClassificationRow],
    ) -> dict[str, int]:
        created = 0
        updated = 0
        for row in rows:
            t = normalize_ticker(row.ticker)
            if not t:
                continue
            existing = session.query(TickerClassification).filter(TickerClassification.ticker == t).one_or_none()
            if existing is None:
                session.add(
                    TickerClassification(
                        ticker=t,
                        sector=row.sector,
                        industry=row.industry,
                        as_of_date=row.as_of_date,
                        source=row.source or "manual",
                        updated_at=dt.datetime.now(dt.timezone.utc),
                    )
                )
                created += 1
            else:
                existing.sector = row.sector
                existing.industry = row.industry
                existing.as_of_date = row.as_of_date
                existing.source = row.source or existing.source or "manual"
                existing.updated_at = dt.datetime.now(dt.timezone.utc)
                updated += 1
        return {"created": created, "updated": updated}

