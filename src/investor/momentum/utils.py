from __future__ import annotations

import re
from typing import Iterable


_TICKER_RE = re.compile(r"[^A-Z0-9.^_-]+")


def normalize_ticker(value: str) -> str:
    s = (value or "").strip().upper()
    if not s:
        return ""
    s = _TICKER_RE.sub("", s)
    return s[:32]


def parse_ticker_list(raw: str) -> list[str]:
    """
    Parses a comma/space/newline-separated ticker list into a de-duped list preserving order.
    """
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[\s,;]+", s)
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        t = normalize_ticker(p)
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def chunked(items: Iterable[str], n: int) -> list[list[str]]:
    n = max(1, int(n))
    chunk: list[str] = []
    out: list[list[str]] = []
    for it in items:
        chunk.append(it)
        if len(chunk) >= n:
            out.append(chunk)
            chunk = []
    if chunk:
        out.append(chunk)
    return out

