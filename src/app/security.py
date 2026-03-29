"""Security middleware - response headers and IP allowlisting."""
from __future__ import annotations

import logging
import os
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


def _parse_allowed_ips() -> Optional[set[str]]:
    """Parse APP_ALLOWED_IPS into an exact-match set."""
    raw = os.environ.get("APP_ALLOWED_IPS", "").strip()
    if not raw:
        return None
    ips = {ip.strip() for ip in raw.split(",") if ip.strip()}
    return ips if ips else None


def _client_ip(request: Request) -> str:
    """Extract client IP, optionally trusting X-Forwarded-For."""
    trust_proxy = os.environ.get("APP_AUTH_TRUST_PROXY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if trust_proxy:
        xff = request.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers and optional IP allowlisting."""

    async def dispatch(self, request: Request, call_next) -> Response:
        allowed = _parse_allowed_ips()
        if allowed is not None:
            client = _client_ip(request)
            if client not in allowed:
                logger.warning("Blocked request from %s (not in APP_ALLOWED_IPS)", client)
                return Response(content="Forbidden", status_code=403, media_type="text/plain")

        response = await call_next(request)
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "connect-src 'self'; "
            "frame-ancestors 'none'"
        )
        if os.environ.get("APP_ENABLE_HSTS", "").strip().lower() in {"1", "true", "yes", "on"}:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response
