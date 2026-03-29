"""Sprint 52 - Security and access control tests."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from fastapi.testclient import TestClient
from starlette.requests import Request

import pytest

from src.app.rate_limit import _BUCKETS, _LOCK, check_rate_limit
from src.app.security import SecurityHeadersMiddleware, _parse_allowed_ips


def _header_app() -> tuple[FastAPI, TestClient]:
    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware)

    @app.get("/test")
    def _test():
        return PlainTextResponse("ok")

    return app, TestClient(app)


def test_security_headers_present() -> None:
    _, client = _header_app()
    resp = client.get("/test")
    assert resp.status_code == 200
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert "Content-Security-Policy" in resp.headers
    assert "frame-ancestors 'none'" in resp.headers["Content-Security-Policy"]
    assert resp.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "Permissions-Policy" in resp.headers


def test_hsts_header_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENABLE_HSTS", "true")
    _, client = _header_app()
    resp = client.get("/test")
    assert "Strict-Transport-Security" in resp.headers
    assert "max-age=" in resp.headers["Strict-Transport-Security"]


def test_hsts_header_absent_by_default(monkeypatch) -> None:
    monkeypatch.delenv("APP_ENABLE_HSTS", raising=False)
    _, client = _header_app()
    resp = client.get("/test")
    assert "Strict-Transport-Security" not in resp.headers


def test_ip_allowlist_blocks_unknown(monkeypatch) -> None:
    monkeypatch.setenv("APP_ALLOWED_IPS", "10.0.0.1,10.0.0.2")
    monkeypatch.delenv("APP_AUTH_TRUST_PROXY", raising=False)
    _, client = _header_app()
    resp = client.get("/test")
    assert resp.status_code == 403


def test_ip_allowlist_allows_listed(monkeypatch) -> None:
    monkeypatch.setenv("APP_ALLOWED_IPS", "testclient,127.0.0.1")
    _, client = _header_app()
    resp = client.get("/test")
    assert resp.status_code == 200


def test_ip_allowlist_disabled_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("APP_ALLOWED_IPS", raising=False)
    _, client = _header_app()
    resp = client.get("/test")
    assert resp.status_code == 200


def test_parse_allowed_ips_empty(monkeypatch) -> None:
    monkeypatch.setenv("APP_ALLOWED_IPS", "")
    assert _parse_allowed_ips() is None


@pytest.fixture(autouse=True)
def _clear_buckets():
    with _LOCK:
        _BUCKETS.clear()
    yield
    with _LOCK:
        _BUCKETS.clear()


def _mock_request(client_host: str = "127.0.0.1") -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/test",
        "headers": [],
        "query_string": b"",
        "client": (client_host, 12345),
    }
    return Request(scope)


def test_rate_limit_allows_within_limit() -> None:
    req = _mock_request()
    for _ in range(5):
        check_rate_limit(req, action="test_action", max_requests=5, window_seconds=60)


def test_rate_limit_blocks_over_limit() -> None:
    req = _mock_request()
    for _ in range(3):
        check_rate_limit(req, action="test_block", max_requests=3, window_seconds=60)
    with pytest.raises(Exception) as exc_info:
        check_rate_limit(req, action="test_block", max_requests=3, window_seconds=60)
    assert exc_info.value.status_code == 429
    assert "Retry-After" in (exc_info.value.headers or {})


def test_rate_limit_per_client_isolation() -> None:
    req_a = _mock_request("10.0.0.1")
    req_b = _mock_request("10.0.0.2")
    for _ in range(3):
        check_rate_limit(req_a, action="test_iso", max_requests=3, window_seconds=60)
    check_rate_limit(req_b, action="test_iso", max_requests=3, window_seconds=60)


def test_rate_limit_disabled_when_zero() -> None:
    req = _mock_request()
    for _ in range(100):
        check_rate_limit(req, action="test_disabled", max_requests=0, window_seconds=60)


def test_rate_limit_per_action_isolation() -> None:
    req = _mock_request()
    for _ in range(3):
        check_rate_limit(req, action="action_a", max_requests=3, window_seconds=60)
    check_rate_limit(req, action="action_b", max_requests=3, window_seconds=60)


def test_security_check_warns_no_password(monkeypatch) -> None:
    from src.app.startup_checks import check_security_config

    monkeypatch.delenv("APP_PASSWORD", raising=False)
    monkeypatch.setenv("APP_SECRET_KEY", "test-key")
    warnings = check_security_config()
    assert any("APP_PASSWORD" in warning for warning in warnings)


def test_security_check_warns_weak_password(monkeypatch) -> None:
    from src.app.startup_checks import check_security_config

    monkeypatch.setenv("APP_PASSWORD", "changeme")
    monkeypatch.setenv("APP_SECRET_KEY", "test-key")
    warnings = check_security_config()
    assert any("weak" in warning.lower() for warning in warnings)


def test_security_check_warns_no_secret_key(monkeypatch) -> None:
    from src.app.startup_checks import check_security_config

    monkeypatch.setenv("APP_PASSWORD", "strong-password-123")
    monkeypatch.delenv("APP_SECRET_KEY", raising=False)
    warnings = check_security_config()
    assert any("APP_SECRET_KEY" in warning for warning in warnings)


def test_security_check_clean_when_all_set(monkeypatch) -> None:
    from src.app.startup_checks import check_security_config

    monkeypatch.setenv("APP_PASSWORD", "strong-unique-password-xyz")
    monkeypatch.setenv("APP_SECRET_KEY", "a-proper-fernet-key")
    warnings = check_security_config()
    assert len(warnings) == 0
