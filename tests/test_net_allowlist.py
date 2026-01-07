from __future__ import annotations

import pytest

from src.core import net
from src.importers.adapters import ProviderError


def test_allowed_outbound_hosts_default_includes_ib_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALLOWED_OUTBOUND_HOSTS", raising=False)
    hosts = net.allowed_outbound_hosts()
    assert "ndcdyn.interactivebrokers.com" in hosts
    assert "gdcdyn.interactivebrokers.com" in hosts
    assert "www.interactivebrokers.com" in hosts


def test_allowed_outbound_hosts_normalizes_url_path_and_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ALLOWED_OUTBOUND_HOSTS",
        "https://ndcdyn.interactivebrokers.com/Universal/servlet/,"
        "www.interactivebrokers.com:443,"
        "gdcdyn.interactivebrokers.com/Universal/servlet/",
    )
    hosts = net.allowed_outbound_hosts()
    assert hosts == {
        "ndcdyn.interactivebrokers.com",
        "www.interactivebrokers.com",
        "gdcdyn.interactivebrokers.com",
    }


def test_assert_url_allowed_mentions_override_when_env_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALLOWED_OUTBOUND_HOSTS", "www.interactivebrokers.com")
    with pytest.raises(ProviderError) as e:
        net.assert_url_allowed("https://ndcdyn.interactivebrokers.com/Universal/servlet/")
    assert "overrides defaults" in str(e.value)

