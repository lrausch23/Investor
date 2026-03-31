#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PY="${PY:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PY" ]]; then
  echo "Python interpreter not found at $PY" >&2
  exit 1
fi

exec "$PY" -m mypy src/regime
