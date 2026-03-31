from __future__ import annotations

import subprocess
from pathlib import Path


PROJECT_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")


def test_mypy_ini_keeps_only_expected_ignore_sections() -> None:
    content = (PROJECT_ROOT / "mypy.ini").read_text(encoding="utf-8")
    assert content.count("ignore_errors = True") == 6
    for module in (
        "[mypy-src.regime.persistence]",
        "[mypy-src.regime.paper_trading]",
        "[mypy-src.regime.llm_layer]",
        "[mypy-src.regime.streamlit_app]",
        "[mypy-src.regime.ib_live_backend]",
        "[mypy-src.importers.adapters]",
    ):
        assert module in content
    assert "[mypy-src.regime.notifications]\nignore_errors = True" not in content


def test_typecheck_script_exists_and_runs() -> None:
    script = PROJECT_ROOT / "scripts" / "typecheck.sh"
    assert script.exists()
    assert script.stat().st_mode & 0o111
    result = subprocess.run([str(script)], cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr


def test_makefile_check_runs_typecheck() -> None:
    content = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "typecheck:" in content
    assert "scripts/typecheck.sh" in content
