from __future__ import annotations

import json
import logging
import smtplib
import urllib.request
from email.mime.text import MIMEText

from .persistence import get_setting

logger = logging.getLogger(__name__)


def get_notification_channels() -> dict[str, bool]:
    return {
        "in_app": True,
        "email": get_setting("notify_email_enabled") == "true",
        "slack": get_setting("notify_slack_enabled") == "true",
    }


def send_email_notification(subject: str, body: str) -> bool:
    host = get_setting("notify_email_smtp_host")
    port = int(get_setting("notify_email_smtp_port") or "587")
    from_addr = get_setting("notify_email_from")
    to_addr = get_setting("notify_email_to")
    password = get_setting("notify_email_password")
    if not all([host, from_addr, to_addr]):
        logger.debug("Email notification skipped — not configured")
        return False
    try:
        msg = MIMEText(body)
        msg["Subject"] = f"[Investor Alert] {subject}"
        msg["From"] = from_addr
        msg["To"] = to_addr
        with smtplib.SMTP(host, port, timeout=10) as server:
            server.ehlo()
            server.starttls()
            if password:
                server.login(from_addr, password)
            server.send_message(msg)
        logger.info("Email notification sent: %s", subject)
        return True
    except Exception as exc:
        logger.warning("Email notification failed: %s", exc)
        return False


def send_slack_notification(text: str) -> bool:
    webhook_url = get_setting("notify_slack_webhook_url")
    if not webhook_url:
        logger.debug("Slack notification skipped — webhook not configured")
        return False
    try:
        data = json.dumps({"text": f":rotating_light: {text}"}).encode("utf-8")
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return int(getattr(resp, "status", 0) or 0) == 200
    except Exception as exc:
        logger.warning("Slack notification failed: %s", exc)
        return False


def dispatch_notification(alert_type: str, title: str, message: str, severity: str = "info") -> dict[str, bool]:
    del alert_type
    results: dict[str, bool] = {"in_app": True}
    channels = get_notification_channels()
    if severity not in ("warning", "critical"):
        return results
    text = f"[{severity.upper()}] {title}\n{message}"
    if channels.get("email"):
        results["email"] = send_email_notification(title, text)
    if channels.get("slack"):
        results["slack"] = send_slack_notification(text)
    return results
