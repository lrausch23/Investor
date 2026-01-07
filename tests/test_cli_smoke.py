from __future__ import annotations

import importlib


def test_market_data_cli_imports():
    # CLI is intentionally minimal for now; ensure module loads.
    importlib.import_module("market_data.cli")

