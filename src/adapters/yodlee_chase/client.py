from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Callable, Optional

from src.core.net import assert_url_allowed, network_enabled
from src.importers.adapters import ProviderError


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    content: bytes
    headers: dict[str, str]


class _AllowlistRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        assert_url_allowed(str(newurl))
        return super().redirect_request(req, fp, code, msg, headers, newurl)


Transport = Callable[[str, str, dict[str, str], bytes | None, float], HttpResponse]


def _default_transport(url: str, method: str, headers: dict[str, str], body: bytes | None, timeout_s: float) -> HttpResponse:
    if not network_enabled():
        raise ProviderError("Network disabled; set NETWORK_ENABLED=1 to enable live connectors.")
    assert_url_allowed(url)

    opener = urllib.request.build_opener(_AllowlistRedirectHandler())
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        if k and v is not None:
            req.add_header(str(k), str(v))
    try:
        with opener.open(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            hdrs = {str(k): str(v) for k, v in dict(resp.headers).items()}
            return HttpResponse(status_code=status, content=resp.read(), headers=hdrs)
    except urllib.error.HTTPError as e:
        # Treat HTTP errors as a response so the caller can handle retries/401 cleanly.
        try:
            content = e.read()  # type: ignore[no-untyped-call]
        except Exception:
            content = b""
        hdrs: dict[str, str] = {}
        try:
            if getattr(e, "headers", None) is not None:
                hdrs = {str(k): str(v) for k, v in dict(e.headers).items()}
        except Exception:
            hdrs = {}
        status = int(getattr(e, "code", 0) or 0)
        return HttpResponse(status_code=status, content=content, headers=hdrs)


class YodleeChaseClient:
    """
    Minimal Yodlee REST client for read-only account/holdings/transactions fetch.

    This client is intentionally tolerant to schema differences:
    - Endpoint path names vary across Yodlee deployments/versions.
    - Response envelope keys may differ (accounts vs account, transactions vs transaction).
    """

    def __init__(
        self,
        *,
        base_url: str,
        access_token: str,
        api_version: str | None = None,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        backoff_s: float = 0.5,
        transport: Transport | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ):
        self.base_url = (base_url or "").strip().rstrip("/")
        if not self.base_url:
            raise ProviderError("Yodlee base URL is required.")
        self.access_token = (access_token or "").strip()
        if not self.access_token:
            raise ProviderError("Missing Yodlee access token; link the connection first.")
        self.api_version = (api_version or "").strip() or None
        self.timeout_s = float(timeout_s)
        self.max_retries = max(0, int(max_retries))
        self.backoff_s = float(backoff_s)
        self._transport = transport or _default_transport
        self._sleep = sleep_fn or time.sleep

        self.rate_limit_hits = 0

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        p = (path or "").strip()
        if not p.startswith("/"):
            p = "/" + p
        url = self.base_url + p
        if params:
            q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            if q:
                url = url + ("&" if "?" in url else "?") + q
        return url

    def _headers(self) -> dict[str, str]:
        h = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        if self.api_version:
            h["Api-Version"] = self.api_version
        return h

    def _request_json(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self._url(path, params=params)
        # Never include full URL or headers in errors (tokens can be in headers; query may include ids).
        parsed = urllib.parse.urlparse(url)
        host = (parsed.hostname or "").lower()
        safe_path = parsed.path or "/"

        attempt = 0
        while True:
            resp = self._transport(url, method.upper(), self._headers(), None, self.timeout_s)
            status = int(resp.status_code or 0)
            if status == 429 or status >= 500:
                if status == 429:
                    self.rate_limit_hits += 1
                if attempt >= self.max_retries:
                    raise ProviderError(f"HTTP error status={status} host={host} path={safe_path}")
                retry_after_s: float | None = None
                try:
                    ra = (resp.headers or {}).get("Retry-After")
                    if ra:
                        retry_after_s = float(str(ra).strip())
                except Exception:
                    retry_after_s = None
                delay = retry_after_s if retry_after_s is not None else min(8.0, self.backoff_s * (2**attempt))
                self._sleep(delay)
                attempt += 1
                continue

            if status in {401, 403}:
                raise ProviderError("Unauthorized; token expired/invalid. Update credentials and try again.")
            if status < 200 or status >= 300:
                raise ProviderError(f"HTTP error status={status} host={host} path={safe_path}")

            try:
                raw = resp.content.decode("utf-8", errors="replace")
                data = json.loads(raw) if raw.strip() else {}
            except Exception:
                raise ProviderError(f"Invalid JSON response host={host} path={safe_path}")
            if not isinstance(data, dict):
                raise ProviderError(f"Unexpected JSON shape host={host} path={safe_path}")
            return data

    def get_accounts(self) -> dict[str, Any]:
        return self._request_json("GET", "/accounts")

    def get_holdings(self, *, account_id: str) -> dict[str, Any]:
        return self._request_json("GET", "/holdings", params={"accountId": account_id})

    def get_transactions(self, *, account_id: str, start_date: str, end_date: str, skip: int, top: int) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/transactions",
            params={"accountId": account_id, "fromDate": start_date, "toDate": end_date, "skip": skip, "top": top},
        )

