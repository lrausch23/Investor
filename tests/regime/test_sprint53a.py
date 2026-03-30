from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = PROJECT_ROOT / "src" / "app" / "static"
VENDOR_DIR = STATIC_DIR / "vendor"
TEMPLATE_DIR = PROJECT_ROOT / "src" / "app" / "templates"
REGIME_JS = STATIC_DIR / "regime.js"
REGIME_HTML = TEMPLATE_DIR / "regime.html"


def test_plotly_vendor_file_exists() -> None:
    plotly_path = VENDOR_DIR / "plotly-2.35.0.min.js"
    assert plotly_path.exists(), f"Missing vendored Plotly at {plotly_path}"


def test_plotly_vendor_file_has_reasonable_size() -> None:
    plotly_path = VENDOR_DIR / "plotly-2.35.0.min.js"
    if not plotly_path.exists():
        pytest.skip("Plotly vendor file not found")
    assert plotly_path.stat().st_size > 3 * 1024 * 1024


def test_regime_template_uses_local_plotly_path() -> None:
    html = REGIME_HTML.read_text(encoding="utf-8")
    assert "cdn.plot.ly" not in html
    assert "vendor/plotly-2.35.0.min.js" in html


def test_regime_js_contains_plotly_fallback_notice() -> None:
    js = REGIME_JS.read_text(encoding="utf-8")
    assert "Plotly library unavailable" in js
    assert "static/vendor/plotly-2.35.0.min.js" in js


def test_render_charts_guard_uses_nested_plotly_check() -> None:
    js = REGIME_JS.read_text(encoding="utf-8")
    match = re.search(r"function renderChartsForTicker\(ticker\)\s*\{", js)
    assert match
    window = js[match.start():match.start() + 2500]
    assert "row && row.charts && window.Plotly" not in window
    assert "if (window.Plotly)" in window
