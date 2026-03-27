from __future__ import annotations

from io import BytesIO
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _fmt_currency(value: Any, digits: int = 2, fallback: str = "—") -> str:
    num = _safe_float(value)
    if num is None:
        return fallback
    return f"${num:,.{digits}f}"


def _fmt_int_currency(value: Any, fallback: str = "—") -> str:
    num = _safe_float(value)
    if num is None:
        return fallback
    return f"${num:,.0f}"


def _fmt_pct(value: Any, digits: int = 1, fallback: str = "—") -> str:
    num = _safe_float(value)
    if num is None:
        return fallback
    return f"{num:.{digits}f}%"


def _styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Small", parent=styles["Normal"], fontSize=8, leading=10))
    styles.add(ParagraphStyle(name="Tiny", parent=styles["Normal"], fontSize=7, leading=9))
    return styles


def _footer_factory(payload: dict[str, Any]):
    generated_at = str(payload.get("generated_at") or payload.get("last_run_display") or "Unknown")
    portfolio_scope = str(payload.get("portfolio_scope") or "portfolio")
    benchmark = str(payload.get("benchmark") or "Unknown")

    def _add_footer(canvas, doc) -> None:  # type: ignore[no-untyped-def]
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(colors.gray)
        footer_text = (
            f"Generated {generated_at} · {portfolio_scope} · Benchmark: {benchmark} · "
            "For internal use only. Model outputs are informational and do not constitute investment advice."
        )
        canvas.drawString(doc.leftMargin, 0.5 * inch, footer_text[:140])
        canvas.drawRightString(doc.pagesize[0] - doc.rightMargin, 0.5 * inch, f"Page {doc.page}")
        canvas.restoreState()

    return _add_footer


def _summary_table(payload: dict[str, Any]) -> Table:
    portfolio_summary = payload.get("portfolio_summary") or {}
    data = [
        ["Metric", "Value"],
        ["Holdings Analyzed", str(payload.get("selected_count") or 0)],
        ["Portfolio Market Value", _fmt_int_currency(payload.get("total_market_value"), "$0")],
        ["Action Items", str(payload.get("action_items_count") or 0)],
        ["Benchmark Regime", str(payload.get("benchmark_regime") or "Unavailable")],
    ]
    if portfolio_summary:
        data.extend(
            [
                [
                    "Regime Exposure",
                    f"{_fmt_pct(portfolio_summary.get('bull_pct'), 1, '0.0%')} Bull · "
                    f"{_fmt_pct(portfolio_summary.get('neutral_pct'), 1, '0.0%')} Neutral · "
                    f"{_fmt_pct(portfolio_summary.get('bear_pct'), 1, '0.0%')} Bear",
                ],
                ["Transition Risk", _fmt_pct(portfolio_summary.get("transition_risk_pct"), 1)],
                ["Diversification", str(portfolio_summary.get("diversification_score") or "—")],
            ]
        )
    table = Table(data, colWidths=[2.1 * inch, 4.4 * inch], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3f4f6")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("LEADING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    return table


def _ticker_table(rows: list[dict[str, Any]]) -> Table:
    data = [[
        "Ticker", "Regime", "Prob%", "Signal", "Action", "Price", "Entry", "Exit", "Stop", "R:R", "Tax", "Mkt Value", "Rel. Str."
    ]]
    for row in rows:
        targets = row.get("price_targets") if isinstance(row.get("price_targets"), dict) else {}
        data.append(
            [
                str(row.get("ticker") or "—"),
                str(row.get("regime") or "—"),
                _fmt_pct(row.get("probability_pct"), 1),
                str(row.get("composite_signal") or "—"),
                str(row.get("action") or "—"),
                _fmt_currency(row.get("current_price")),
                _fmt_currency(targets.get("entry_price") if targets else None),
                _fmt_currency(targets.get("exit_price") if targets else None),
                _fmt_currency(targets.get("stop_price") if targets else None),
                f"{_safe_float(targets.get('risk_reward_ratio')):.2f}" if _safe_float(targets.get("risk_reward_ratio")) is not None else "—",
                str(row.get("tax_status") or "—"),
                _fmt_int_currency(row.get("market_value")),
                str(row.get("relative_strength") or "—"),
            ]
        )
    table = Table(
        data,
        colWidths=[0.55 * inch, 0.62 * inch, 0.52 * inch, 0.62 * inch, 0.52 * inch, 0.6 * inch, 0.6 * inch, 0.6 * inch, 0.6 * inch, 0.42 * inch, 0.62 * inch, 0.72 * inch, 0.65 * inch],
        repeatRows=1,
        hAlign="LEFT",
    )
    styles = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#333333")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("LEADING", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    for idx in range(1, len(data)):
        if idx % 2 == 0:
            styles.append(("BACKGROUND", (0, idx), (-1, idx), colors.HexColor("#fafafa")))
        regime = str(data[idx][1])
        signal = str(data[idx][3])
        action = str(data[idx][4])
        if regime == "Bull":
            styles.append(("BACKGROUND", (1, idx), (1, idx), colors.Color(0.85, 0.95, 0.85)))
        elif regime == "Bear":
            styles.append(("BACKGROUND", (1, idx), (1, idx), colors.Color(0.95, 0.85, 0.85)))
        else:
            styles.append(("BACKGROUND", (1, idx), (1, idx), colors.Color(0.93, 0.93, 0.93)))
        if "Buy" in signal:
            styles.append(("BACKGROUND", (3, idx), (3, idx), colors.Color(0.85, 0.95, 0.85)))
        elif "Sell" in signal:
            styles.append(("BACKGROUND", (3, idx), (3, idx), colors.Color(0.95, 0.85, 0.85)))
        elif "Hold" in signal:
            styles.append(("BACKGROUND", (3, idx), (3, idx), colors.Color(1.0, 1.0, 0.85)))
        if "Buy" in action:
            styles.append(("BACKGROUND", (4, idx), (4, idx), colors.Color(0.85, 0.95, 0.85)))
        elif "Sell" in action:
            styles.append(("BACKGROUND", (4, idx), (4, idx), colors.Color(0.95, 0.85, 0.85)))
        elif "Hold" in action:
            styles.append(("BACKGROUND", (4, idx), (4, idx), colors.Color(1.0, 1.0, 0.85)))
    table.setStyle(TableStyle(styles))
    return table


def _lot_tables(rows: list[dict[str, Any]], styles) -> list[Any]:
    story: list[Any] = []
    for row in rows:
        lot_details = row.get("lot_details") or []
        if not lot_details:
            continue
        story.append(PageBreak())
        story.append(Paragraph(f"{row.get('ticker', 'Ticker')} — Tax Lot Details", styles["Heading2"]))
        story.append(Spacer(1, 0.1 * inch))
        for signal in row.get("account_tax_signals") or []:
            story.append(
                Paragraph(
                    f"{signal.get('account_name', 'Unknown')} / {signal.get('account_type', 'Unknown')}: "
                    f"{signal.get('adjusted_action', '—')} · {signal.get('tax_note', '')}",
                    styles["Small"],
                )
            )
        if row.get("account_tax_signals"):
            story.append(Spacer(1, 0.08 * inch))
        data = [["Account", "Acquired", "Qty", "Cost Basis", "Term", "Days to LTCG"]]
        for lot in lot_details:
            data.append(
                [
                    str(lot.get("account_name") or "—"),
                    str(lot.get("acquisition_date") or "—"),
                    f"{_safe_float(lot.get('qty')):,.3f}" if _safe_float(lot.get("qty")) is not None else "—",
                    _fmt_currency(lot.get("cost_basis") if lot.get("cost_basis") is not None else lot.get("basis_total")),
                    str(lot.get("term") or "—"),
                    str(lot.get("days_to_ltcg") if lot.get("days_to_ltcg") is not None else "—"),
                ]
            )
        table = Table(data, colWidths=[1.6 * inch, 1.1 * inch, 0.8 * inch, 1.1 * inch, 0.6 * inch, 0.9 * inch], repeatRows=1, hAlign="LEFT")
        ts = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3f4f6")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
        ]
        for idx, lot in enumerate(lot_details, start=1):
            if lot.get("near_ltcg"):
                ts.append(("BACKGROUND", (0, idx), (-1, idx), colors.Color(0.98, 0.92, 0.92)))
            term = str(lot.get("term") or "").upper()
            if term == "LT":
                ts.append(("TEXTCOLOR", (4, idx), (4, idx), colors.HexColor("#166534")))
            elif term == "ST":
                ts.append(("TEXTCOLOR", (4, idx), (4, idx), colors.HexColor("#b91c1c")))
        table.setStyle(TableStyle(ts))
        story.append(table)
    return story


def generate_regime_pdf(payload: dict[str, Any]) -> bytes:
    styles = _styles()
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.8 * inch,
    )
    rows = list(payload.get("rows") or [])
    generated_at = str(payload.get("generated_at") or payload.get("last_run_display") or payload.get("last_run_timestamp") or "Unknown")
    metadata_bits = [
        f"Generated: {generated_at}",
        f"Benchmark: {payload.get('benchmark', '—')} ({payload.get('benchmark_regime', '—')})",
        f"Period: {payload.get('period', '—')}",
        f"Portfolio: {payload.get('portfolio_scope', '—')}",
        f"Mode: {payload.get('portfolio_mode', '—')}",
    ]
    if payload.get("account_id") is not None:
        metadata_bits.append(f"Account: {payload.get('account_id')}")

    story: list[Any] = [
        Paragraph("REGIME ANALYSIS REPORT", styles["Title"]),
        Spacer(1, 0.08 * inch),
        Paragraph(" | ".join(metadata_bits), styles["Small"]),
        Spacer(1, 0.18 * inch),
        Paragraph("Portfolio Summary", styles["Heading2"]),
        Spacer(1, 0.08 * inch),
        _summary_table(payload),
        Spacer(1, 0.18 * inch),
        Paragraph("Ticker Regime Table", styles["Heading2"]),
        Spacer(1, 0.08 * inch),
        _ticker_table(rows),
    ]

    if any((row.get("lot_details") or []) for row in rows):
        story.extend(_lot_tables(rows, styles))

    action_items = ((payload.get("digest") or {}).get("action_items") or [])
    if action_items:
        story.append(PageBreak())
        story.append(Paragraph("Action Items", styles["Heading2"]))
        story.append(Spacer(1, 0.08 * inch))
        for item in action_items:
            story.append(Paragraph(f"• {item}", styles["Normal"]))

    footer = _footer_factory(payload)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return buffer.getvalue()
