from __future__ import annotations

from pathlib import Path

from src.app.routes import regime as regime_route

from tests import test_regime_route as route_tests


TEMPLATE_PATH = Path("/Volumes/T9/Projects/Dev/Investor/src/app/templates/regime.html")
JS_PATH = Path("/Volumes/T9/Projects/Dev/Investor/src/app/static/regime.js")
CSS_PATH = Path("/Volumes/T9/Projects/Dev/Investor/src/app/static/app.css")


def _rendered_regime_html(monkeypatch) -> str:
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "warnings": []})
    client = route_tests._client(monkeypatch)
    response = client.get("/regime")
    assert response.status_code == 200
    return response.text


def test_tabs_render_three_buttons(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert 'data-regime-tab="analysis"' in html
    assert 'data-regime-tab="trading"' in html
    assert 'data-regime-tab="research"' in html


def test_analysis_tab_active_by_default(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert 'class="regime-tab regime-tab--active" data-regime-tab="analysis"' in html
    assert 'class="regime-tab-panel regime-tab-panel--active" data-regime-panel="analysis"' in html


def test_template_has_three_tab_panels(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert 'data-regime-panel="analysis"' in html
    assert 'data-regime-panel="trading"' in html
    assert 'data-regime-panel="research"' in html


def test_hash_navigation_prefers_hash_over_saved_tab() -> None:
    js = JS_PATH.read_text()
    assert 'const baseHash = hash.split("/")[0];' in js
    assert 'const target = baseHash || prefs.activeTab || "analysis";' in js


def test_hero_strip_exists(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert 'id="regimeHeroStrip"' in html
    assert 'id="regimeHeroValue"' in html
    assert 'id="regimeHeroStats"' in html


def test_control_grid_uses_two_column_layout() -> None:
    css = CSS_PATH.read_text()
    assert ".regime-control-grid" in css
    assert "grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);" in css


def test_holdings_aside_removed(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert "holdings-aside" not in html


def test_theme_drawer_exists(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert 'id="regimeThemeDrawer"' in html
    assert 'id="regimeThemeDrawerOverlay"' in html
    assert 'id="regimeManageThemes"' in html


def test_holding_chips_use_regime_classes() -> None:
    js = JS_PATH.read_text()
    assert "regime-chip--bull" in js
    assert "regime-chip--bear" in js
    assert "regime-chip--neutral" in js


def test_action_badge_uses_semantic_class() -> None:
    js = JS_PATH.read_text()
    css = CSS_PATH.read_text()
    assert "regime-badge--action" in js
    assert ".regime-badge--action" in css


def test_table_has_regime_table_class() -> None:
    js = JS_PATH.read_text()
    assert 'class="table-wrap regime-table"' in js
    assert 'class="regime-table"' in js


def test_table_row_polish_exists_in_css() -> None:
    css = CSS_PATH.read_text()
    assert ".regime-table tbody tr:nth-child(even):not(.is-total-row) td" in css


def test_status_bar_defaults_hidden_in_template(monkeypatch) -> None:
    html = _rendered_regime_html(monkeypatch)
    assert 'id="regimeStatusBar" style="display:none"' in html


def test_status_bar_visibility_logic_targets_ibkr() -> None:
    js = JS_PATH.read_text()
    assert 'String(portfolio.broker_type || "").toLowerCase() === "ibkr"' in js
    assert 'bar.style.display = "flex";' in js
    assert 'bar.style.display = "none";' in js


def test_prefs_key_and_column_toggle_persistence_exist() -> None:
    js = JS_PATH.read_text()
    assert 'const PREFS_KEY = "regime_ui_prefs";' in js
    assert 'savePref("showAllColumns", state.showAllTableColumns);' in js


def test_tab_and_drawer_preferences_are_persisted() -> None:
    js = JS_PATH.read_text()
    assert 'savePref("activeTab", target);' in js
    assert 'savePref("themeDrawerOpen", !!open);' in js


def test_status_bar_and_tabs_css_exist() -> None:
    css = CSS_PATH.read_text()
    assert ".regime-status-bar" in css
    assert ".regime-tabs" in css
    assert ".regime-tab-panel--active" in css
