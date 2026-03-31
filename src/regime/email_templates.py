from __future__ import annotations

from typing import Any


def _severity_color(severity: str) -> str:
    normalized = str(severity or "info").lower()
    if normalized == "critical":
        return "#c62828"
    if normalized == "warning":
        return "#e65100"
    return "#1565c0"


def render_critical_alert_email(
    alert_type: str,
    title: str,
    message: str,
    severity: str,
    ticker: str | None,
    created_at: str,
    data: dict[str, Any] | None = None,
) -> tuple[str, str]:
    severity_color = _severity_color(severity)
    ticker_line = f"<p><strong>Ticker:</strong> {ticker}</p>" if ticker else ""
    data_section = ""
    if data:
        rows = "".join(
            f"<li><strong>{key}:</strong> {value}</li>"
            for key, value in sorted(data.items())
        )
        data_section = f"<ul>{rows}</ul>"
    html = f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px; margin: 0 auto;">
  <div style="background: {severity_color}; color: white; padding: 12px 20px; border-radius: 8px 8px 0 0;">
    <h2 style="margin: 0;">⚠️ {title}</h2>
  </div>
  <div style="border: 1px solid #e0e0e0; border-top: none; padding: 20px; border-radius: 0 0 8px 8px;">
    <p><strong>Type:</strong> {alert_type}</p>
    <p><strong>Severity:</strong> {severity}</p>
    {ticker_line}
    <p><strong>Time:</strong> {created_at}</p>
    <hr style="border: none; border-top: 1px solid #eee;">
    <p>{message}</p>
    {data_section}
  </div>
  <p style="color: #999; font-size: 12px; text-align: center; margin-top: 16px;">
    Investor Portfolio Management System
  </p>
</div>
""".strip()
    text_lines = [
        title,
        f"Type: {alert_type}",
        f"Severity: {severity}",
        f"Time: {created_at}",
    ]
    if ticker:
        text_lines.append(f"Ticker: {ticker}")
    text_lines.extend(["", message])
    if data:
        text_lines.extend(["", "Context:"])
        text_lines.extend([f"- {key}: {value}" for key, value in sorted(data.items())])
    return html, "\n".join(text_lines)


def render_digest_email(
    alerts: list[dict[str, Any]],
    period_label: str = "Daily",
) -> tuple[str, str]:
    ordered = sorted(
        alerts,
        key=lambda item: ({"critical": 0, "warning": 1, "info": 2}.get(str(item.get("severity") or "info"), 3), str(item.get("created_at") or "")),
    )
    critical_count = sum(1 for item in ordered if str(item.get("severity") or "") == "critical")
    warning_count = sum(1 for item in ordered if str(item.get("severity") or "") == "warning")
    info_count = sum(1 for item in ordered if str(item.get("severity") or "") == "info")
    rows = []
    text_rows = []
    for item in ordered:
        severity = str(item.get("severity") or "info")
        bg = {"critical": "#ffebee", "warning": "#fff3e0", "info": "#e3f2fd"}.get(severity, "#f5f5f5")
        rows.append(
            f"<tr style=\"background:{bg}\"><td>{item.get('created_at','')[:16]}</td><td>{severity}</td><td>{item.get('alert_type','')}</td><td>{item.get('title','')}</td><td>{item.get('ticker') or ''}</td></tr>"
        )
        text_rows.append(
            f"{item.get('created_at','')[:16]} | {severity} | {item.get('alert_type','')} | {item.get('title','')} | {item.get('ticker') or ''}"
        )
    html = f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 760px; margin: 0 auto;">
  <h2>{period_label} Alert Digest</h2>
  <p>{len(ordered)} alerts ({critical_count} critical, {warning_count} warning, {info_count} info)</p>
  <table style="width:100%; border-collapse: collapse;">
    <thead><tr><th align="left">Time</th><th align="left">Severity</th><th align="left">Type</th><th align="left">Title</th><th align="left">Ticker</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
""".strip()
    text = "\n".join(
        [
            f"{period_label} Alert Digest",
            f"{len(ordered)} alerts ({critical_count} critical, {warning_count} warning, {info_count} info)",
            "",
            *text_rows,
        ]
    )
    return html, text
