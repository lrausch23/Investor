#!/usr/bin/env bash
# Investor Launch Script
# Usage: ./scripts/launch.sh [--start-gateway] [--port 8000] [--host 0.0.0.0]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

HOST="${INVESTOR_HOST:-0.0.0.0}"
PORT="${INVESTOR_PORT:-8000}"
START_GATEWAY=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start-gateway) START_GATEWAY=true; shift ;;
        --port) PORT="$2"; shift 2 ;;
        --host) HOST="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== Investor Launch ==="
echo "Project: $PROJECT_DIR"
echo "Host: $HOST  Port: $PORT"
echo

echo "[1/4] Validating environment..."
python -m src.app.startup_checks
echo "  Environment OK"

if [ "$START_GATEWAY" = true ]; then
    echo "[2/4] Starting IB Gateway..."
    if [ -d "$PROJECT_DIR/IBGW" ]; then
        "$PROJECT_DIR/IBGW/ibgateway" &
        GATEWAY_PID=$!
        echo "  Gateway PID: $GATEWAY_PID"
    else
        echo "  WARNING: IBGW/ directory not found. Skipping gateway start."
    fi
else
    echo "[2/4] Skipping IB Gateway start (use --start-gateway to enable)"
fi

echo "[3/4] Checking IB Gateway connectivity..."
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_DIR/.env"
    set +a
fi
GW_PORT="${IBKR_PORT:-7497}"
GW_HOST="${IBKR_HOST:-127.0.0.1}"

RETRIES=0
MAX_RETRIES=15
while [ $RETRIES -lt $MAX_RETRIES ]; do
    if nc -z "$GW_HOST" "$GW_PORT" 2>/dev/null; then
        echo "  IB Gateway reachable on $GW_HOST:$GW_PORT"
        break
    fi
    RETRIES=$((RETRIES + 1))
    if [ $RETRIES -eq $MAX_RETRIES ]; then
        echo "  WARNING: IB Gateway not reachable on $GW_HOST:$GW_PORT after ${MAX_RETRIES} attempts"
        echo "  Server will start but IBKR features will use fallback providers"
        break
    fi
    sleep 2
done

echo "[4/4] Starting Investor server..."
exec uvicorn src.app.main:app --host "$HOST" --port "$PORT" --log-level info
