from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import inspect
from typing import Any
from zoneinfo import ZoneInfo

try:
    import aiosmtplib
except Exception:  # pragma: no cover - optional dependency fallback
    aiosmtplib = None  # type: ignore[assignment]

import httpx

from .email_templates import render_critical_alert_email, render_digest_email
from .persistence import get_channels_for_alert, get_notification_preferences, get_setting

logger = logging.getLogger(__name__)

_email_rate_tracker: dict[str, float] = {}
_digest_buffer: list[dict[str, Any]] = []
EMAIL_RATE_LIMIT_SECONDS = 3600


def _setting_or_env(setting_key: str, env_key: str) -> str:
    return str(get_setting(setting_key) or os.getenv(env_key) or "").strip()


def _now_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def get_notification_channels() -> dict[str, bool]:
    email_configured = bool(
        _setting_or_env("notify_email_smtp_host", "SMTP_HOST")
        and _setting_or_env("notify_email_from", "SMTP_FROM")
        and _setting_or_env("notify_email_to", "SMTP_TO")
    )
    slack_configured = bool(
        _setting_or_env("notify_slack_webhook_url", "SLACK_WEBHOOK_URL")
    )
    return {
        "in_app": True,
        "email": email_configured,
        "slack": slack_configured,
    }


def _email_rate_limited(alert_type: str) -> bool:
    last_sent = _email_rate_tracker.get(str(alert_type), 0.0)
    return (time.time() - last_sent) < EMAIL_RATE_LIMIT_SECONDS


def _email_rate_record(alert_type: str) -> None:
    _email_rate_tracker[str(alert_type)] = time.time()


def _is_quiet_hours() -> bool:
    start = str(get_setting("notify_quiet_hours_start") or "").strip()
    end = str(get_setting("notify_quiet_hours_end") or "").strip()
    if not start or not end:
        return False
    tz_name = str(get_setting("notify_quiet_hours_tz") or "America/New_York").strip() or "America/New_York"
    zone = ZoneInfo(tz_name)
    now_local = datetime.now(zone).time()
    start_time = datetime.strptime(start, "%H:%M").time()
    end_time = datetime.strptime(end, "%H:%M").time()
    if start_time <= end_time:
        return start_time <= now_local <= end_time
    return now_local >= start_time or now_local <= end_time


def buffer_for_digest(alert: dict[str, Any]) -> None:
    _digest_buffer.append(dict(alert))


async def flush_digest() -> bool:
    global _digest_buffer
    if not _digest_buffer:
        return False
    html_body, text_body = render_digest_email(_digest_buffer)
    success = await send_email_notification(
        subject=f"Daily Alert Digest — {len(_digest_buffer)} alerts",
        body_html=html_body,
        body_text=text_body,
    )
    if success:
        _digest_buffer = []
    return success


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def send_email_notification(
    subject: str,
    body_html: str,
    *,
    body_text: str | None = None,
    to_override: str | None = None,
) -> bool:
    host = _setting_or_env("notify_email_smtp_host", "SMTP_HOST")
    from_addr = _setting_or_env("notify_email_from", "SMTP_FROM")
    to_addr = str(to_override or _setting_or_env("notify_email_to", "SMTP_TO")).strip()
    username = _setting_or_env("notify_email_user", "SMTP_USER") or from_addr
    password = _setting_or_env("notify_email_password", "SMTP_PASSWORD")
    port = int(_setting_or_env("notify_email_smtp_port", "SMTP_PORT") or "587")
    use_tls = str(get_setting("notify_email_use_tls") or "true").lower() in {"true", "1", "yes", "on"}
    if not host or not from_addr or not to_addr:
        logger.debug("Email not configured")
        return False
    if aiosmtplib is None:
        logger.warning("Email notification skipped: aiosmtplib not installed")
        return False
    msg = MIMEMultipart("alternative")
    subject_prefix = "[Investor Alert]" if "critical" in str(subject).lower() else "[Investor]"
    msg["Subject"] = f"{subject_prefix} {subject}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(body_text or body_html, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))
    try:
        await aiosmtplib.send(
            msg,
            hostname=host,
            port=port,
            start_tls=use_tls,
            username=username or None,
            password=password or None,
            timeout=15,
        )
        logger.info("Email notification sent: %s", subject)
        return True
    except Exception as exc:
        logger.warning("Email notification failed: %s", exc)
        return False


def _build_slack_blocks(
    title: str,
    message: str,
    severity: str,
    alert_type: str | None,
    ticker: str | None,
) -> dict[str, Any]:
    severity_colors = {"critical": "#c62828", "warning": "#e65100", "info": "#1565c0"}
    severity_emoji = {"critical": "🔴", "warning": "🟠", "info": "🔵"}
    normalized = str(severity or "info").lower()
    fields: list[dict[str, str]] = []
    if alert_type:
        fields.append({"type": "mrkdwn", "text": f"*Type:* `{alert_type}`"})
    if ticker:
        fields.append({"type": "mrkdwn", "text": f"*Ticker:* `{ticker}`"})
    fields.append({"type": "mrkdwn", "text": f"*Severity:* {severity_emoji.get(normalized, 'ℹ️')} {normalized.upper()}"})
    return {
        "attachments": [
            {
                "color": severity_colors.get(normalized, "#1565c0"),
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": title[:150], "emoji": True}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": message[:3000]}, "fields": fields},
                    {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Investor Alert • {_now_str()}"}]},
                ],
            }
        ]
    }


async def send_slack_notification(
    title: str,
    message: str,
    *,
    severity: str = "info",
    alert_type: str | None = None,
    ticker: str | None = None,
    webhook_url_override: str | None = None,
) -> bool:
    normalized = str(severity or "info").lower()
    webhook_url = str(
        webhook_url_override
        or (get_setting("notify_slack_webhook_critical") if normalized == "critical" else "")
        or get_setting("notify_slack_webhook_url")
        or os.getenv("SLACK_WEBHOOK_URL")
        or ""
    ).strip()
    if not webhook_url:
        logger.debug("Slack notification skipped — webhook not configured")
        return False
    payload = _build_slack_blocks(title, message, normalized, alert_type, ticker)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(webhook_url, json=payload)
        if response.status_code == 200:
            return True
        logger.warning("Slack notification failed with status %s", response.status_code)
        return False
    except Exception as exc:
        logger.warning("Slack notification failed: %s", exc)
        return False


async def dispatch_notification_async(
    alert_type: str,
    title: str,
    message: str,
    severity: str = "info",
    *,
    ticker: str | None = None,
    data: dict[str, Any] | None = None,
) -> dict[str, bool | str]:
    results: dict[str, bool | str] = {"in_app": True}
    channels = get_channels_for_alert(alert_type)
    quiet_hours = _is_quiet_hours()
    digest_enabled = str(get_setting("notify_digest_enabled") or "false").lower() in {"true", "1", "yes", "on"}

    if str(severity or "info").lower() == "info":
        return results

    if "email" in channels:
        if quiet_hours and severity != "critical":
            results["email"] = "quiet_hours"
        elif digest_enabled and severity != "critical":
            buffer_for_digest({"alert_type": alert_type, "title": title, "message": message, "severity": severity, "ticker": ticker, "data": data, "created_at": _now_str()})
            results["email"] = "buffered"
        elif _email_rate_limited(alert_type):
            results["email"] = "rate_limited"
        else:
            html_body, text_body = render_critical_alert_email(
                alert_type=alert_type,
                title=title,
                message=message,
                severity=severity,
                ticker=ticker,
                created_at=_now_str(),
                data=data,
            )
            sent = bool(await _maybe_await(send_email_notification(title, html_body, body_text=text_body)))
            results["email"] = sent
            if sent:
                _email_rate_record(alert_type)

    if "slack" in channels:
        if quiet_hours and severity != "critical":
            results["slack"] = "quiet_hours"
        else:
            results["slack"] = bool(
                await _maybe_await(
                    send_slack_notification(
                        title=title,
                        message=message,
                        severity=severity,
                        alert_type=alert_type,
                        ticker=ticker,
                    )
                )
            )

    return results


def dispatch_notification(
    alert_type: str,
    title: str,
    message: str,
    severity: str = "info",
    **kwargs: Any,
) -> dict[str, bool | str]:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        asyncio.ensure_future(dispatch_notification_async(alert_type, title, message, severity, **kwargs))
        return {"in_app": True, "email": "scheduled", "slack": "scheduled"}
    return asyncio.run(dispatch_notification_async(alert_type, title, message, severity, **kwargs))


def dispatch_notification_sync(
    alert_type: str,
    title: str,
    message: str,
    severity: str = "info",
    **kwargs: Any,
) -> dict[str, bool | str]:
    return dispatch_notification(alert_type, title, message, severity, **kwargs)


def notification_preferences_payload() -> dict[str, Any]:
    channels = get_notification_channels()
    return {
        "preferences": get_notification_preferences(),
        "settings": {
            "quiet_hours_start": str(get_setting("notify_quiet_hours_start") or ""),
            "quiet_hours_end": str(get_setting("notify_quiet_hours_end") or ""),
            "quiet_hours_tz": str(get_setting("notify_quiet_hours_tz") or "America/New_York"),
            "digest_enabled": str(get_setting("notify_digest_enabled") or "false").lower() in {"true", "1", "yes", "on"},
            "email_configured": channels["email"],
            "slack_configured": channels["slack"],
        },
    }
