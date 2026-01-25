from __future__ import annotations

import os
import ssl
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


def outbound_host_allowlist_enabled() -> bool:
    """
    Safety control: outbound host allowlist is enabled by default.

    To disable (unsafe), set:
      - DISABLE_OUTBOUND_HOST_ALLOWLIST=1
    """
    raw = (os.environ.get("ALLOWED_OUTBOUND_HOSTS") or "").strip()
    if raw:
        # Explicit allowlist should always be enforced, even if disable is set.
        return True
    v = (os.environ.get("DISABLE_OUTBOUND_HOST_ALLOWLIST") or "").strip().lower()
    return v not in {"1", "true", "yes", "y", "on"}


def _normalize_allowlisted_host(entry: str) -> str | None:
    """
    Normalize a user-provided allowlist entry to a hostname.

    Accepts:
      - bare hostnames: "ndcdyn.interactivebrokers.com"
      - host:port: "example.com:443"
      - host/path: "example.com/some/path"
      - full URLs: "https://example.com/some/path"
    """
    s = (entry or "").strip()
    if not s:
        return None

    # Full URL
    if "://" in s:
        try:
            u = urllib.parse.urlparse(s)
            host = (u.hostname or "").strip().lower()
            return host or None
        except Exception:
            return None

    # Strip path (host/path)
    if "/" in s:
        s = s.split("/", 1)[0].strip()

    # Support IPv6 literals: "[::1]:443"
    if s.startswith("[") and "]" in s:
        inside = s[1 : s.index("]")]
        return inside.strip().lower() or None

    # host:port (or accidental scheme-less URL-ish input)
    if ":" in s:
        try:
            u = urllib.parse.urlparse(f"https://{s}")
            host = (u.hostname or "").strip().lower()
            return host or None
        except Exception:
            s = s.split(":", 1)[0].strip()

    s = s.strip().lower()
    return s or None


def allowed_outbound_hosts() -> set[str]:
    raw = (os.environ.get("ALLOWED_OUTBOUND_HOSTS") or "").strip()
    if raw:
        hosts: list[str] = []
        for part in raw.split(","):
            h = _normalize_allowlisted_host(part)
            if h:
                hosts.append(h)
        return set(hosts)
    # Safe-by-default allowlist: IB Flex Web Service hosts only.
    return {
        # Some IB environments use ndcdyn (as seen in the IB Portal UI); others use gdcdyn.
        "ndcdyn.interactivebrokers.com",
        "gdcdyn.interactivebrokers.com",
        "www.interactivebrokers.com",
        # Market data (end-of-day) used by holdings refresh and momentum screener.
        "stooq.com",
        "finnhub.io",
        # Plaid API (sync connections, e.g., Chase OAuth + transactions).
        "sandbox.plaid.com",
        "production.plaid.com",
    }


def _assert_url_allowed(url: str) -> None:
    u = urllib.parse.urlparse(url)
    if (u.scheme or "").lower() != "https":
        raise ProviderError("Blocked network request: only https:// is allowed.")
    host = (u.hostname or "").lower()
    if not host:
        raise ProviderError("Blocked network request: missing hostname.")
    if not outbound_host_allowlist_enabled():
        return
    allowed = allowed_outbound_hosts()
    if host not in allowed:
        raw = (os.environ.get("ALLOWED_OUTBOUND_HOSTS") or "").strip()
        if raw:
            hint = "ALLOWED_OUTBOUND_HOSTS is set and overrides defaults; add this host (or unset the variable)."
        else:
            hint = "Set ALLOWED_OUTBOUND_HOSTS to include this host."
        preview = ",".join(sorted(list(allowed))[:8])
        suffix = "…" if len(allowed) > 8 else ""
        extra = f" Allowed: [{preview}{suffix}]." if allowed else ""
        raise ProviderError(f"Blocked network request: host not allowlisted ({host}). {hint}{extra}")


def assert_url_allowed(url: str) -> None:
    _assert_url_allowed(url)


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    content: bytes
    content_type: Optional[str] = None
    headers: Optional[dict[str, str]] = None


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
    verify_tls: bool = True,
    raise_for_status: bool = True,
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
    last_status: int | None = None
    max_backoff_s = 60.0
    try:
        max_backoff_s = float(os.environ.get("HTTP_MAX_BACKOFF_S", "60"))
    except Exception:
        max_backoff_s = 60.0
    while attempt <= max_retries:
        try:
            ctx = ssl.create_default_context() if verify_tls else ssl._create_unverified_context()
            opener = urllib.request.build_opener(_AllowlistRedirectHandler(), urllib.request.HTTPSHandler(context=ctx))
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
            last_status = status
            if not raise_for_status:
                try:
                    content = e.read() or b""
                except Exception:
                    content = b""
                try:
                    content_type = e.headers.get("Content-Type") if getattr(e, "headers", None) is not None else None
                except Exception:
                    content_type = None
                try:
                    hdrs = {str(k): str(v) for k, v in dict(e.headers).items()} if getattr(e, "headers", None) is not None else None
                except Exception:
                    hdrs = None
                return HttpResponse(status_code=status, content=content, content_type=content_type, headers=hdrs)
            if status == 429 or status >= 500:
                time.sleep(min(max_backoff_s, backoff_s * (2**attempt)))
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
    if last_status is not None:
        try:
            u = urllib.parse.urlparse(url)
            host = (u.hostname or "").lower()
            path = u.path or "/"
            if int(last_status) == 429:
                raise ProviderError(
                    f"Rate limited by remote host (HTTP 429) host={host} path={path}. "
                    "Wait a few minutes and retry, or upload the benchmark CSV manually."
                )
            raise ProviderError(f"Network request failed after retries: HTTP error status={last_status} host={host} path={path}")
        except ProviderError:
            raise
    raise ProviderError(f"Network request failed after retries: {type(last_err).__name__ if last_err else 'unknown'}")


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    body: bytes | None = None,
    timeout_s: float = 30.0,
    max_retries: int = 2,
    backoff_s: float = 0.5,
    verify_tls: bool = True,
    raise_for_status: bool = True,
) -> HttpResponse:
    """
    Minimal HTTP helper with:
      - NETWORK_ENABLED gate
      - outbound host allowlist
      - headers + body support
      - timeouts + limited retries (429/5xx)

    Never include secrets in raised errors; callers should avoid logging full URLs or request headers.
    """
    if not network_enabled():
        raise ProviderError("Network disabled; set NETWORK_ENABLED=1 to enable live connectors.")
    _assert_url_allowed(url)

    m = (method or "GET").strip().upper()
    if m not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
        raise ProviderError(f"Unsupported HTTP method: {m}")

    attempt = 0
    last_err: Exception | None = None
    last_status: int | None = None
    last_host: str | None = None
    last_path: str | None = None
    last_body_preview: str | None = None
    last_reason: str | None = None
    max_backoff_s = 60.0
    try:
        max_backoff_s = float(os.environ.get("HTTP_MAX_BACKOFF_S", "60"))
    except Exception:
        max_backoff_s = 60.0
    while attempt <= max_retries:
        try:
            ctx = ssl.create_default_context() if verify_tls else ssl._create_unverified_context()
            opener = urllib.request.build_opener(_AllowlistRedirectHandler(), urllib.request.HTTPSHandler(context=ctx))
            req = urllib.request.Request(url, data=body, method=m)
            for k, v in (headers or {}).items():
                if k and v is not None:
                    req.add_header(str(k), str(v))
            with opener.open(req, timeout=timeout_s) as resp:
                status = int(getattr(resp, "status", 200))
                content_type = resp.headers.get("Content-Type")
                hdrs = {str(k): str(v) for k, v in dict(resp.headers).items()}
                content = resp.read()
                return HttpResponse(status_code=status, content=content, content_type=content_type, headers=hdrs)
        except urllib.error.HTTPError as e:
            last_err = e
            last_reason = None
            status = int(getattr(e, "code", 0) or 0)
            last_status = status
            if not raise_for_status:
                try:
                    content = e.read() or b""
                except Exception:
                    content = b""
                try:
                    content_type = e.headers.get("Content-Type") if getattr(e, "headers", None) is not None else None
                except Exception:
                    content_type = None
                try:
                    hdrs = {str(k): str(v) for k, v in dict(e.headers).items()} if getattr(e, "headers", None) is not None else None
                except Exception:
                    hdrs = None
                return HttpResponse(status_code=status, content=content, content_type=content_type, headers=hdrs)
            try:
                u = urllib.parse.urlparse(url)
                last_host = (u.hostname or "").lower()
                last_path = u.path or "/"
            except Exception:
                last_host = None
                last_path = None
            retry_after_s: float | None = None
            try:
                ra = e.headers.get("Retry-After") if getattr(e, "headers", None) is not None else None
                if ra:
                    retry_after_s = float(str(ra).strip())
            except Exception:
                retry_after_s = None
            try:
                b = e.read()
                if isinstance(b, (bytes, bytearray)) and b:
                    last_body_preview = b[:200].decode("utf-8", errors="replace").strip()
            except Exception:
                last_body_preview = None
            if status == 429 or status >= 500:
                delay = retry_after_s if retry_after_s is not None else min(max_backoff_s, backoff_s * (2**attempt))
                time.sleep(delay)
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
                last_reason = str(getattr(e, "reason", "") or str(e)).strip() or None
            except Exception:
                last_reason = None
            try:
                u = urllib.parse.urlparse(url)
                last_host = (u.hostname or "").lower() or last_host
                last_path = u.path or last_path
            except Exception:
                pass
            time.sleep(min(8.0, backoff_s * (2**attempt)))
            attempt += 1
            continue
        except Exception as e:
            last_err = e
            try:
                last_reason = str(e).strip() or None
            except Exception:
                last_reason = None
            time.sleep(min(8.0, backoff_s * (2**attempt)))
            attempt += 1
            continue

    if last_status is not None:
        host = f" host={last_host}" if last_host else ""
        path = f" path={last_path}" if last_path else ""
        preview = f" body={last_body_preview!r}" if last_body_preview else ""
        if int(last_status) == 429:
            raise ProviderError(
                f"Rate limited by remote host (HTTP 429){host}{path}{preview}. "
                "Wait a few minutes and retry, or upload the benchmark CSV manually."
            )
        raise ProviderError(f"Network request failed after retries: HTTP error status={last_status}{host}{path}{preview}")
    host = f" host={last_host}" if last_host else ""
    path = f" path={last_path}" if last_path else ""
    reason = f" reason={last_reason!r}" if last_reason else ""
    raise ProviderError(
        f"Network request failed after retries: {type(last_err).__name__ if last_err else 'unknown'}{host}{path}{reason}"
    )
