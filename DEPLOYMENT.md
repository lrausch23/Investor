# Deployment

## Prerequisites

- Python 3.11+
- IB Gateway or TWS with API enabled
- At least one LLM API key:
  - `OPENAI_API_KEY`
  - `GOOGLE_API_KEY`
  - `ANTHROPIC_API_KEY`

## Quick Start

```bash
cd /Volumes/T9/Projects/Dev/Investor
./scripts/launch.sh
```

Start with a custom host/port:

```bash
./scripts/launch.sh --host 0.0.0.0 --port 8000
```

Start and attempt to launch IB Gateway too:

```bash
./scripts/launch.sh --start-gateway
```

## Environment Variables

| Variable | Purpose |
|---|---|
| `IBKR_HOST` | IB Gateway / TWS host |
| `IBKR_PORT` | IB Gateway / TWS API port |
| `IBKR_CLIENT_ID` | Main app IBKR client id |
| `IBKR_ACCOUNT_ID` | Paper or primary account id |
| `IBKR_LIVE_ACCOUNT_ID` | Live account id for unlock checks |
| `IBKR_LIVE_BACKEND` | Enable live backend (`true`/`false`) |
| `IBKR_TIMEOUT` | IBKR connection timeout seconds |
| `OPENAI_API_KEY` | OpenAI access |
| `GOOGLE_API_KEY` | Gemini access |
| `ANTHROPIC_API_KEY` | Claude access |
| `INVESTOR_HOST` | Default host for `launch.sh` |
| `INVESTOR_PORT` | Default port for `launch.sh` |

## macOS Setup

Install the launchd plist:

```bash
cp scripts/com.investor.app.plist ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/com.investor.app.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.investor.app.plist
launchctl start com.investor.app
```

Check logs:

```bash
tail -f /tmp/investor.stdout.log
tail -f /tmp/investor.stderr.log
```

## Linux Setup

Install the systemd unit:

```bash
sudo cp scripts/investor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable investor.service
sudo systemctl start investor.service
```

Check status and logs:

```bash
sudo systemctl status investor.service
journalctl -u investor.service -f
```

## Manual Start

Run startup checks only:

```bash
python -m src.app.startup_checks
```

Start the app directly:

```bash
uvicorn src.app.main:app --host 0.0.0.0 --port 8000 --log-level info
```

## Troubleshooting

1. IB Gateway not reachable:

```bash
nc -z 127.0.0.1 7497
```

2. Missing API keys:

```bash
grep -E 'OPENAI_API_KEY|GOOGLE_API_KEY|ANTHROPIC_API_KEY' .env
```

3. Port conflict on app startup:

```bash
lsof -i :8000
```

4. IBKR settings saved but not reflected:

```bash
grep '^IBKR_' .env
```

5. Launch script fails environment checks:

```bash
python -m src.app.startup_checks
```

## Backup And Recovery

Sprint 49 added automated backup and recovery tooling. Use the app health and backup routes for operational checks, and preserve the `data/` directory as part of host-level backups.
