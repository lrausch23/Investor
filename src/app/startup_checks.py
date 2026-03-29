"""Pre-flight environment validation for Investor."""
from __future__ import annotations

import os
import socket
import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def check_env_file() -> list[str]:
    """Verify .env file exists and has required IBKR keys."""
    errors: list[str] = []
    env_path = _project_root() / ".env"
    if not env_path.exists():
        errors.append(f".env file not found at {env_path}")
        return errors

    content = env_path.read_text(encoding="utf-8")
    keys_found: set[str] = set()
    for line in content.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            keys_found.add(stripped.split("=", 1)[0].strip())

    for key in ("IBKR_HOST", "IBKR_PORT", "IBKR_ACCOUNT_ID"):
        if key not in keys_found:
            errors.append(f"Missing required key in .env: {key}")
    return errors


def check_api_keys() -> list[str]:
    """Verify at least one LLM provider API key is configured."""
    llm_keys = ("OPENAI_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY")
    if any(os.environ.get(key, "").strip() for key in llm_keys):
        return []
    return ["No LLM API key found. Set at least one of: " + ", ".join(llm_keys)]


def check_database_paths() -> list[str]:
    """Verify database directories are accessible."""
    warnings: list[str] = []
    project_dir = _project_root()
    if not project_dir.exists():
        return [f"Project directory not found: {project_dir}"]

    regime_db_dir = project_dir / "data" / "regime"
    if not regime_db_dir.exists():
        warnings.append(
            f"Regime data directory not found: {regime_db_dir}. It will be created on first regime run."
        )
    return warnings


def check_python_dependencies() -> list[str]:
    """Verify critical Python packages are importable."""
    errors: list[str] = []
    critical_packages = [
        ("fastapi", "FastAPI"),
        ("uvicorn", "Uvicorn"),
        ("ib_insync", "ib_insync (IBKR connectivity)"),
        ("xgboost", "XGBoost (meta-labeler)"),
    ]
    for module_name, display_name in critical_packages:
        try:
            __import__(module_name)
        except ImportError:
            errors.append(f"Missing Python package: {display_name} (pip install {module_name})")
    return errors


def check_ibkr_gateway() -> list[str]:
    """Check if IB Gateway port is reachable (non-blocking, informational)."""
    warnings: list[str] = []
    host = os.environ.get("IBKR_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("IBKR_PORT", "7497"))
    except ValueError:
        port = 7497

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        if sock.connect_ex((host, port)) != 0:
            warnings.append(
                f"IB Gateway not reachable on {host}:{port}. IBKR features will use fallback providers."
            )
    except Exception as exc:
        warnings.append(f"IB Gateway check failed: {exc}")
    finally:
        sock.close()
    return warnings


def check_security_config() -> list[str]:
    """Check security-critical configuration."""
    warnings: list[str] = []
    password = os.environ.get("APP_PASSWORD", "").strip()
    if not password:
        warnings.append(
            "APP_PASSWORD is not set. Authentication is DISABLED. Set APP_PASSWORD in .env for production use."
        )
    elif password in ("changeme", "password", "admin", "investor"):
        warnings.append(
            "APP_PASSWORD is set to a weak default value. Use a strong, unique password for production."
        )

    secret_key = os.environ.get("APP_SECRET_KEY", "").strip()
    if not secret_key:
        warnings.append(
            "APP_SECRET_KEY is not set. Credential encryption uses a fallback key. Set a strong random value in .env for production."
        )
    return warnings


def run_all_checks() -> tuple[list[str], list[str]]:
    """Run all pre-flight checks. Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    errors.extend(check_env_file())
    errors.extend(check_api_keys())
    errors.extend(check_python_dependencies())
    warnings.extend(check_database_paths())
    warnings.extend(check_ibkr_gateway())
    warnings.extend(check_security_config())
    return errors, warnings


if __name__ == "__main__":
    env_path = _project_root() / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key, value = stripped.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

    errors, warnings = run_all_checks()
    if warnings:
        print("WARNINGS:")
        for warning in warnings:
            print(f"  ! {warning}")
    if errors:
        print("ERRORS:")
        for error in errors:
            print(f"  x {error}")
        sys.exit(1)
    print("All pre-flight checks passed.")
    sys.exit(0)
