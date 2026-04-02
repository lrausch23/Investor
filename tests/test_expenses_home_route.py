from __future__ import annotations

from fastapi.testclient import TestClient

from src.app.main import create_app


def test_expenses_home_renders_without_now_utc_shadowing_error() -> None:
    client = TestClient(create_app())
    response = client.get("/expenses")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
