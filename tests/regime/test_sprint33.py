from __future__ import annotations

from pathlib import Path
import socket
from types import SimpleNamespace

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from tests import test_regime_route as route_tests


def _client() -> TestClient:
    return TestClient(create_app())


def test_ibkr_settings_get(monkeypatch) -> None:
    monkeypatch.setenv("IBKR_PORT", "7497")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    client = _client()
    response = client.get("/regime/ibkr/settings")
    assert response.status_code == 200
    payload = response.json()
    assert "config" in payload
    assert "readiness" in payload
    assert payload["config"]["port"] == 7497
    assert payload["config"]["account_id"] == "DUP579027"


def test_ibkr_settings_update_paper_port(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("FOO=bar\nIBKR_PORT=4002\n", encoding="utf-8")
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_path)
    client = _client()
    response = client.post(
        "/regime/ibkr/settings",
        data={
            "host": "127.0.0.1",
            "port": "7497",
            "client_id": "1",
            "account_id": "DUP579027",
            "live_backend": "false",
            "timeout": "10",
        },
    )
    assert response.status_code == 200
    assert response.json()["saved"] is True
    contents = env_path.read_text(encoding="utf-8")
    assert "FOO=bar" in contents
    assert "IBKR_PORT=7497" in contents


def test_ibkr_settings_accepts_live_port(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("IBKR_PORT=7497\nIBKR_ACCOUNT_ID=DUP579027\n", encoding="utf-8")
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_path)
    client = _client()
    response = client.post("/regime/ibkr/settings", data={"port": "7496", "account_id": "DUP579027"})
    assert response.status_code == 200


def test_ibkr_settings_accepts_non_du_with_live(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("IBKR_PORT=7497\nIBKR_ACCOUNT_ID=DUP579027\n", encoding="utf-8")
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_path)
    client = _client()
    response = client.post(
        "/regime/ibkr/settings",
        data={"port": "7496", "account_id": "DUP579027", "live_account_id": "U123456", "live_backend": "true"},
    )
    assert response.status_code == 200


def test_ibkr_test_connection_no_tws(monkeypatch) -> None:
    monkeypatch.setattr(socket, "create_connection", lambda *args, **kwargs: (_ for _ in ()).throw(ConnectionRefusedError()))
    client = _client()
    response = client.post("/regime/ibkr/test-connection")
    assert response.status_code == 200
    payload = response.json()
    assert payload["tcp_reachable"] is False
    assert "Cannot reach" in payload["error"]


def test_ibkr_test_connection_success(monkeypatch) -> None:
    class _Sock:
        def close(self):
            return None

    class _Backend:
        def __init__(self, account_id):
            self.account_id = account_id

        def connect(self, host, port, client_id):
            return True

        def get_account_summary(self):
            return SimpleNamespace(account_id="DUP579027", net_liquidation=1_000_000.0)

        def disconnect(self):
            return None

    monkeypatch.setattr(socket, "create_connection", lambda *args, **kwargs: _Sock())
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    import src.regime.ib_live_backend as live_backend

    monkeypatch.setattr(live_backend, "LiveIBBackend", _Backend)
    client = _client()
    response = client.post("/regime/ibkr/test-connection")
    assert response.status_code == 200
    payload = response.json()
    assert payload["tcp_reachable"] is True
    assert payload["ibkr_connected"] is True
    assert payload["account_verified"] is True


def test_identify_threshold_path_bear_strong_sell() -> None:
    path = regime_route._identify_threshold_path(regime="Bear", transition_risk=0.03, technical_signal="Stay defensive")
    assert "Strong Sell" in path
    assert "transition_risk < 0.05" in path


def test_identify_threshold_path_bear_sell() -> None:
    path = regime_route._identify_threshold_path(regime="Bear", transition_risk=0.10, technical_signal="Stay defensive")
    assert "Sell path" in path


def test_identify_threshold_path_bear_hold_fallback() -> None:
    path = regime_route._identify_threshold_path(regime="Bear", transition_risk=0.20, technical_signal="Stay defensive")
    assert "Hold fallback" in path
    assert "transition_risk >= 0.15" in path


def test_identify_threshold_path_bear_tactical_override() -> None:
    path = regime_route._identify_threshold_path(regime="Bear", transition_risk=0.20, technical_signal="Cover short / tactical bounce")
    assert "Hold fallback" in path
    assert "Bear tactical override" in path


def test_identify_threshold_path_bull_strong_buy() -> None:
    path = regime_route._identify_threshold_path(regime="Bull", transition_risk=0.03, technical_signal="Accumulate")
    assert "Strong Buy" in path


def test_docs_uat_section_present(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "warnings": []})
    client = route_tests._client(monkeypatch)
    response = client.get("/docs")
    assert response.status_code == 200
    assert 'id="uat-paper-trading"' in response.text
    assert "UAT — Paper Trading" in response.text
    assert response.text.count('class="uat-step__check uat-check"') >= 20


def test_signal_diagnostics_in_payload(monkeypatch) -> None:
    client = route_tests._client(monkeypatch)
    run_response = client.post("/regime/run", data={"tickers": "NVDA,AVGO", "benchmark": "SOXX", "period": "3y"})
    assert run_response.status_code == 200
    job_id = run_response.json()["job_id"]
    status_response = client.get(f"/regime/status/{job_id}")
    assert status_response.status_code == 200
    payload = status_response.json()["payload"]
    rows = payload["rows"]
    assert rows
    diagnostics = rows[0]["signal_diagnostics"]
    assert diagnostics["forward_action"] is not None
    assert "forward_transition_risk" in diagnostics
    assert "technical_signal" in diagnostics
    assert "composite_action" in diagnostics
    assert "thresholds_applied" in diagnostics
