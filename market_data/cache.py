from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from market_data.symbols import sanitize_ticker

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CacheMetadata:
    provider: str
    original_ticker: str
    provider_ticker: str
    auto_adjust: bool
    first_date: str | None
    last_date: str | None
    fetched_at: str | None
    rows: int | None
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "provider": self.provider,
            "original_ticker": self.original_ticker,
            "provider_ticker": self.provider_ticker,
            "auto_adjust": bool(self.auto_adjust),
            "first_date": self.first_date,
            "last_date": self.last_date,
            "fetched_at": self.fetched_at,
            "rows": self.rows,
        }


class PriceCache:
    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def sanitize_ticker(self, ticker: str) -> str:
        return sanitize_ticker(ticker)

    def _base_path(self, ticker: str) -> Path:
        return self.cache_dir / self.sanitize_ticker(ticker)

    def _parquet_path(self, ticker: str) -> Path:
        return self._base_path(ticker).with_suffix(".parquet")

    def _csv_path(self, ticker: str) -> Path:
        return self._base_path(ticker).with_suffix(".csv")

    def _meta_path(self, ticker: str) -> Path:
        return self._base_path(ticker).with_suffix(".json")

    def get_metadata(self, ticker: str) -> dict[str, Any] | None:
        p = self._meta_path(ticker)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    def save(self, ticker: str, df, metadata: CacheMetadata) -> None:
        """
        Save a per-ticker DataFrame to Parquet if possible, else CSV.
        Also writes a JSON sidecar metadata file.
        """
        meta_p = self._meta_path(ticker)
        meta_p.write_text(json.dumps(metadata.to_dict(), indent=2, sort_keys=True), encoding="utf-8")

        pq = self._parquet_path(ticker)
        csvp = self._csv_path(ticker)
        # Always save CSV as a dependency-free fallback.
        try:
            df.to_csv(csvp, index=True)
        except Exception as e:
            logger.warning("Failed writing CSV cache for %s: %s", ticker, e)

        try:
            df.to_parquet(pq, index=True)
        except Exception:
            # Parquet is optional.
            if pq.exists():
                try:
                    pq.unlink()
                except Exception:
                    pass

    def load(self, ticker: str):
        """
        Load per-ticker cached DataFrame if present (Parquet preferred, else CSV).
        Returns None if no cache.
        """
        pq = self._parquet_path(ticker)
        csvp = self._csv_path(ticker)
        if pq.exists():
            try:
                import pandas as pd  # type: ignore

                df = pd.read_parquet(pq)
                if "date" in df.columns and df.index.name != "date":
                    try:
                        df = df.set_index("date")
                    except Exception:
                        pass
                return df
            except Exception:
                pass
        if csvp.exists():
            try:
                import pandas as pd  # type: ignore

                df = pd.read_csv(csvp, parse_dates=["date"], index_col="date")
                return df
            except Exception:
                return None
        return None
