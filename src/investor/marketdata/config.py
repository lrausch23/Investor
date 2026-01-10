from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


class BenchmarksCacheConfig(BaseModel):
    type: str = Field(default="sqlite", description="Cache backend type: sqlite")
    path: str = Field(default="data/benchmarks/benchmarks.sqlite", description="SQLite path for candles cache")


class BenchmarksYahooConfig(BaseModel):
    # Yahoo is a last-resort fallback and is disabled by default due to frequent 429s.
    enabled: bool = False
    max_rps: float = 1.0
    max_retries: int = 6
    backoff_base_seconds: float = 2.0


class BenchmarksStooqConfig(BaseModel):
    enabled: bool = True


class BenchmarksConfig(BaseModel):
    # Cache-first: offline-first. Stooq is the default network provider.
    provider_order: list[str] = Field(default_factory=lambda: ["cache", "stooq"])
    cache: BenchmarksCacheConfig = Field(default_factory=BenchmarksCacheConfig)
    yahoo: BenchmarksYahooConfig = Field(default_factory=BenchmarksYahooConfig)
    stooq: BenchmarksStooqConfig = Field(default_factory=BenchmarksStooqConfig)
    benchmark_proxy: str = Field(default="SPY", description="Proxy symbol used for ^GSPC")


class AppMarketDataConfig(BaseModel):
    benchmarks: BenchmarksConfig = Field(default_factory=BenchmarksConfig)


def _candidate_paths() -> list[Path]:
    paths = [Path("benchmarks.yaml")]
    home = Path(os.path.expanduser("~"))
    paths.append(home / ".bucketmgr" / "benchmarks.yaml")
    return paths


def load_marketdata_config() -> tuple[AppMarketDataConfig, Optional[str]]:
    """
    Load market data config from YAML (if present).

    Search paths (first match wins):
      - ./benchmarks.yaml
      - ~/.bucketmgr/benchmarks.yaml
    """
    for p in _candidate_paths():
        if p.exists():
            data = yaml.safe_load(p.read_text()) or {}
            return AppMarketDataConfig.model_validate(data), str(p)
    return AppMarketDataConfig(), None
