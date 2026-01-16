from __future__ import annotations

import re
from dataclasses import dataclass

from src.core.net import http_get
from src.importers.adapters import ProviderError
from src.investor.momentum.utils import normalize_ticker


_STOOQ_TICKER_RE = re.compile(r"(?:\\?|&)s=([A-Za-z0-9._^:-]+)")


@dataclass(frozen=True)
class StooqUniverseResult:
    tickers: list[str]
    source_url: str
    warnings: list[str]


def fetch_stooq_index_components(*, index_symbol: str) -> StooqUniverseResult:
    """
    Fetch index components from Stooq and return a list of normalized tickers.

    Notes:
    - Stooq usually uses symbols like `aapl.us` for US equities/ETFs.
    - This is best-effort: Stooq does not reliably provide sector/industry metadata
      for all indices, so this importer focuses on constituents only.
    """
    idx = (index_symbol or "").strip().lower()
    if not idx:
        raise ProviderError("Missing Stooq index symbol.")

    url = f"https://stooq.com/q/i/?s={idx}"
    resp = http_get(url, timeout_s=30.0, max_retries=2, backoff_s=1.0)
    if int(resp.status_code) != 200:
        raise ProviderError(f"Stooq index request failed: status={resp.status_code}")
    html = resp.content.decode("utf-8", errors="replace")
    if not html:
        raise ProviderError("Stooq returned empty response.")

    # Extract tickers from links like ...?s=aapl.us
    raw_syms = _STOOQ_TICKER_RE.findall(html)
    if not raw_syms:
        raise ProviderError("Could not find any constituents on the Stooq page.")

    tickers: list[str] = []
    warnings: list[str] = []
    for sym in raw_syms:
        s = (sym or "").strip()
        if not s:
            continue
        # Drop `.us` suffix if present; normalize to internal ticker conventions.
        s_u = s.upper()
        if s_u.endswith(".US"):
            s_u = s_u[:-3]
        # Ignore index self-references.
        if s_u in {idx.upper(), f"^{idx.upper()}"}:
            continue
        t = normalize_ticker(s_u)
        if not t:
            continue
        tickers.append(t)

    # Dedupe while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for t in tickers:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)

    if not out:
        raise ProviderError("No usable constituents found after normalization.")

    # Basic sanity: warn if we got an unexpectedly large list (likely parsing noise).
    if len(out) > 2000:
        warnings.append("Stooq constituent list is unusually large; results may include non-constituents.")

    return StooqUniverseResult(tickers=out, source_url=url, warnings=warnings)

