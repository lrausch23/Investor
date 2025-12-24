from __future__ import annotations

import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

from src.importers.adapters import ProviderError


def network_enabled() -> bool:
    v = (os.environ.get("NETWORK_ENABLED") or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def allowed_outbound_hosts() -> set[str]:
    raw = (os.environ.get("ALLOWED_OUTBOUND_HOSTS") or "").strip()
    if raw:
        hosts = [h.strip().lower() for h in raw.split(",") if h.strip()]
        return set(hosts)
    # Safe-by-default allowlist: IB Flex Web Service hosts only.
    return {
        # Some IB environments use ndcdyn (as seen in the IB Portal UI); others use gdcdyn.
        "ndcdyn.interactivebrokers.com",
        "gdcdyn.interactivebrokers.com",
        "www.interactivebrokers.com",
    }


def _assert_url_allowed(url: str) -> None:
    u = urllib.parse.urlparse(url)
    if (u.scheme or "").lower() != "https":
        raise ProviderError("Blocked network request: only https:// is allowed.")
    host = (u.hostname or "").lower()
    if not host:
        raise ProviderError("Blocked network request: missing hostname.")
    if host not in allowed_outbound_hosts():
        raise ProviderError(f"Blocked network request: host not allowlisted ({host}).")


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    content: bytes
    content_type: Optional[str] = None


class _AllowlistRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        # Enforce allowlist on redirects as well.
        _assert_url_allowed(str(newurl))
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def http_get(
    url: str,
    *,
    timeout_s: float = 30.0,
    max_retries: int = 2,
    backoff_s: float = 0.5,
) -> HttpResponse:
    """
    Minimal HTTP GET helper with:
      - NETWORK_ENABLED gate
      - outbound host allowlist
      - timeouts + limited retries

    Never include secrets in raised errors; callers should avoid passing secret URLs to logs.
    """
    if not network_enabled():
        raise ProviderError("Network disabled; set NETWORK_ENABLED=1 to enable live connectors.")
    _assert_url_allowed(url)

    attempt = 0
    last_err: Exception | None = None
    last_host: str | None = None
    last_reason: str | None = None
    while attempt <= max_retries:
        try:
            opener = urllib.request.build_opener(_AllowlistRedirectHandler())
            req = urllib.request.Request(url, method="GET")
            with opener.open(req, timeout=timeout_s) as resp:
                status = int(getattr(resp, "status", 200))
                content_type = resp.headers.get("Content-Type")
                content = resp.read()
                return HttpResponse(status_code=status, content=content, content_type=content_type)
        except urllib.error.HTTPError as e:
            # Treat 5xx as retryable, 4xx as hard fail (except throttling-ish 429).
            last_err = e
            status = int(getattr(e, "code", 0) or 0)
            if status == 429 or status >= 500:
                time.sleep(min(8.0, backoff_s * (2**attempt)))
                attempt += 1
                continue
            try:
                u = urllib.parse.urlparse(url)
                host = (u.hostname or "").lower()
                path = u.path or "/"
                raise ProviderError(f"HTTP error status={status} host={host} path={path}")
            except ProviderError:
                raise
            except Exception:
                raise ProviderError(f"HTTP error status={status}")
        except urllib.error.URLError as e:
            last_err = e
            try:
                last_host = urllib.parse.urlparse(url).hostname
            except Exception:
                last_host = None
            try:
                r = getattr(e, "reason", None)
                last_reason = str(r) if r is not None else str(e)
            except Exception:
                last_reason = None
            time.sleep(min(8.0, backoff_s * (2**attempt)))
            attempt += 1
            continue
        except Exception as e:
            last_err = e
            time.sleep(min(8.0, backoff_s * (2**attempt)))
            attempt += 1
            continue

    if last_reason:
        host = f" host={last_host}" if last_host else ""
        raise ProviderError(f"Network request failed after retries: {type(last_err).__name__}: {last_reason}.{host}".strip())
    raise ProviderError(f"Network request failed after retries: {type(last_err).__name__ if last_err else 'unknown'}")
