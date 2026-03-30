"""Sprint 54 - Code Quality & Technical Debt tests."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
REGIME_DIR = SRC_DIR / "regime"


class TestYFinanceDeduplication:
    def test_market_data_client_exists(self) -> None:
        assert (REGIME_DIR / "market_data_client.py").exists()

    def test_no_direct_yf_import_in_delegating_files(self) -> None:
        targets = [
            REGIME_DIR / "data.py",
            REGIME_DIR / "investor_adapter.py",
            REGIME_DIR / "paper_trading.py",
            REGIME_DIR / "discovery.py",
            REGIME_DIR / "vix_freeze.py",
        ]
        for filepath in targets:
            content = filepath.read_text(encoding="utf-8")
            assert "import yfinance" not in content, f"{filepath.name} still imports yfinance directly"

    def test_market_data_client_has_required_functions(self) -> None:
        from src.regime import market_data_client as mdc

        for fn in ("download_daily_bars", "get_ticker_info", "get_ticker_news", "get_earnings_date", "get_current_vix"):
            assert callable(getattr(mdc, fn, None)), f"Missing function: {fn}"

    def test_download_daily_bars_delegates_to_yf(self) -> None:
        fake_frame = pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2024-01-02"]))
        with patch("src.regime.market_data_client.yf.download", return_value=fake_frame) as mock_dl:
            from src.regime.market_data_client import download_daily_bars

            result = download_daily_bars("AAPL", period="5d")
            mock_dl.assert_called_once()
            assert not result.empty

    def test_get_ticker_info_returns_dict_on_failure(self) -> None:
        with patch("src.regime.market_data_client.yf.Ticker", side_effect=Exception("network")):
            from src.regime.market_data_client import get_ticker_info

            assert get_ticker_info("BAD") == {}


class TestExceptionHierarchy:
    def test_investor_error_is_root(self) -> None:
        from src.regime.exceptions import InvestorError

        assert issubclass(InvestorError, Exception)

    def test_regime_error_inherits_investor_error(self) -> None:
        from src.regime.exceptions import InvestorError, RegimeError

        assert issubclass(RegimeError, InvestorError)

    def test_broker_errors_exist(self) -> None:
        from src.regime.exceptions import BrokerConnectionError, BrokerError, BrokerExecutionError

        assert issubclass(BrokerConnectionError, BrokerError)
        assert issubclass(BrokerExecutionError, BrokerError)

    def test_persistence_reparented(self) -> None:
        from src.regime.exceptions import InvestorError, PersistenceError, RegimeError

        assert issubclass(PersistenceError, InvestorError)
        assert not issubclass(PersistenceError, RegimeError)

    def test_data_validation_error_exists(self) -> None:
        from src.regime.exceptions import DataValidationError, InvestorError

        assert issubclass(DataValidationError, InvestorError)


class TestDependencyPins:
    def test_requirements_uses_exact_pins(self) -> None:
        req_path = PROJECT_ROOT / "requirements.txt"
        for line in req_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            assert "==" in line, f"Not exact-pinned: {line}"

    def test_requirements_range_exists(self) -> None:
        range_path = PROJECT_ROOT / "requirements-range.txt"
        assert range_path.exists()
        assert ">=" in range_path.read_text()


class TestMypyBootstrap:
    def test_mypy_ini_exists(self) -> None:
        assert (PROJECT_ROOT / "mypy.ini").exists()


class TestWarningCleanup:
    def test_no_on_event_startup(self) -> None:
        main_py = SRC_DIR / "app" / "main.py"
        content = main_py.read_text(encoding="utf-8")
        assert "on_event" not in content

    def test_no_utcnow_in_src(self) -> None:
        hits: list[str] = []
        for py_file in SRC_DIR.rglob("*.py"):
            content = py_file.read_text(encoding="utf-8")
            if "utcnow()" in content:
                hits.append(str(py_file.relative_to(PROJECT_ROOT)))
        assert not hits, f"utcnow() still found in: {hits}"
