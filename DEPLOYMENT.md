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

## Security

### Authentication

Set `APP_PASSWORD` in `.env` to enable HTTP Basic authentication.
When set, all UI and API endpoints require a username and password.

```bash
APP_PASSWORD=your-strong-password-here
```

### Credential Encryption

Set `APP_SECRET_KEY` for Fernet encryption of stored credentials:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### HTTPS / TLS

The application does not terminate TLS directly. Deploy behind a reverse
proxy such as nginx, Caddy, or Cloudflare Tunnel for HTTPS.

Example nginx configuration:

```nginx
server {
    listen 443 ssl;
    server_name investor.local;

    ssl_certificate     /etc/ssl/certs/investor.pem;
    ssl_certificate_key /etc/ssl/private/investor-key.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

When using a reverse proxy, set `APP_AUTH_TRUST_PROXY=true` and
`APP_ENABLE_HSTS=true` in `.env`.

### IP Allowlisting

Restrict access to specific IP addresses:

```bash
APP_ALLOWED_IPS=127.0.0.1,192.168.1.100
```

When set, requests from other IPs receive `403 Forbidden`.

### Rate Limiting

Trading endpoints are rate-limited per client IP. Defaults:

| Action | Limit | Window |
|---|---:|---:|
| Trade execute | 10 | 60s |
| Kill switch | 3 | 60s |
| Live unlock | 3 | 300s |
| Auto-approve | 5 | 60s |
| Settings save | 10 | 60s |

Override global defaults with `APP_RATE_LIMIT_MAX` and `APP_RATE_LIMIT_WINDOW`.

### Security Checklist

Before exposing the app to a network:

1. Set `APP_PASSWORD` to a strong value.
2. Set `APP_SECRET_KEY` to a Fernet key.
3. Deploy behind an HTTPS reverse proxy.
4. Set `APP_AUTH_TRUST_PROXY=true`.
5. Set `APP_ENABLE_HSTS=true`.
6. Consider setting `APP_ALLOWED_IPS`.
7. Verify no API keys appear in logs.

## Backup And Recovery

Sprint 49 added automated backup and recovery tooling. Use the app health and backup routes for operational checks, and preserve the `data/` directory as part of host-level backups.
