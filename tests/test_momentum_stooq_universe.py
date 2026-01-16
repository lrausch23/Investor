from __future__ import annotations

import pytest

from src.importers.adapters import ProviderError
from src.investor.momentum.stooq_universe import fetch_stooq_index_components


def test_stooq_universe_parser_extracts_tickers(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html><body>
      <table>
        <tr><th>Symbol</th><th>Name</th></tr>
        <tr><td><a href="/q/?s=aapl.us">AAPL.US</a></td><td>Apple</td></tr>
        <tr><td><a href="/q/?s=msft.us">MSFT.US</a></td><td>Microsoft</td></tr>
        <tr><td><a href="/q/?s=aapl.us">AAPL.US</a></td><td>Apple dup</td></tr>
      </table>
    </body></html>
    """.encode("utf-8")

    class _Resp:
        status_code = 200
        content = html
        content_type = "text/html"

    def fake_http_get(url: str, timeout_s: float = 30.0, max_retries: int = 2, backoff_s: float = 1.0):
        assert "stooq.com/q/i/?s=spx" in url
        return _Resp()

    monkeypatch.setattr("src.investor.momentum.stooq_universe.http_get", fake_http_get)
    res = fetch_stooq_index_components(index_symbol="spx")
    assert res.tickers == ["AAPL", "MSFT"]
    assert res.source_url.endswith("/q/i/?s=spx")


def test_stooq_universe_parser_empty_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        status_code = 200
        content = b"<html></html>"
        content_type = "text/html"

    def fake_http_get(url: str, timeout_s: float = 30.0, max_retries: int = 2, backoff_s: float = 1.0):
        return _Resp()

    monkeypatch.setattr("src.investor.momentum.stooq_universe.http_get", fake_http_get)
    with pytest.raises(ProviderError):
        fetch_stooq_index_components(index_symbol="spx")

