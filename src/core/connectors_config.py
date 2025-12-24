from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass(frozen=True)
class ConfigConnection:
    name: str
    provider: str
    broker: str
    connector: Optional[str]
    taxpayer_name: str
    fixture_dir: Optional[str]
    data_dir: Optional[str]
    token: Optional[str]
    query_id: Optional[str]
    extra_query_ids: Optional[list[str]]


def _candidate_paths() -> list[Path]:
    paths = [Path("connectors.yaml")]
    home = Path(os.path.expanduser("~"))
    paths.append(home / ".bucketmgr" / "connectors.yaml")
    return paths


def load_config_connections() -> tuple[list[ConfigConnection], Optional[str]]:
    for p in _candidate_paths():
        if p.exists():
            data = yaml.safe_load(p.read_text()) or {}
            conns_raw = data.get("connections") or []
            out: list[ConfigConnection] = []
            for c in conns_raw:
                if not isinstance(c, dict):
                    continue
                out.append(
                    ConfigConnection(
                        name=str(c.get("name") or "").strip(),
                        provider=str(c.get("provider") or "YODLEE").strip().upper(),
                        broker=str(c.get("broker") or "IB").strip().upper(),
                        connector=(str(c.get("connector")).strip().upper() if c.get("connector") else None),
                        taxpayer_name=str(c.get("taxpayer") or "Trust").strip(),
                        fixture_dir=(str(c.get("fixture_dir")).strip() if c.get("fixture_dir") else None),
                        data_dir=(str(c.get("data_dir")).strip() if c.get("data_dir") else None),
                        token=(str(c.get("token")).strip() if c.get("token") else None),
                        query_id=(str(c.get("query_id")).strip() if c.get("query_id") else None),
                        extra_query_ids=(
                            [str(x).strip() for x in (c.get("extra_query_ids") or []) if str(x).strip()]
                            if isinstance(c.get("extra_query_ids"), list)
                            else None
                        ),
                    )
                )
            return out, str(p)
    return [], None
