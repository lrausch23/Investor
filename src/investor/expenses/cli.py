from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

from src.db.session import get_session
from src.investor.expenses.categorize import (
    apply_rules_to_db,
    categorize_one,
    load_rules,
    write_starter_rules,
)
from src.investor.expenses.config import load_expenses_config
from src.investor.expenses.db import ImportOptions, ensure_db, import_csv_statement
from src.investor.expenses.importers import default_importers
from src.investor.expenses.importers.base import read_csv_rows
from src.investor.expenses.normalize import normalize_description, normalize_merchant
from src.investor.expenses.recurring import detect_recurring
from src.investor.expenses.reports import (
    budget_vs_actual,
    category_summary,
    format_table,
    merchants_by_spend,
    opportunities,
    render_simple_html,
    write_csv,
)


expenses_app = typer.Typer(help="Expense analysis: import, categorize, recurring, reports.")
rules_app = typer.Typer(help="Manage categorization rules.")
expenses_app.add_typer(rules_app, name="rules")


def _check_runtime() -> None:
    try:
        import sqlalchemy  # noqa: F401
    except Exception as e:
        typer.echo(f"Runtime dependency error: {type(e).__name__}: {e}", err=True)
        raise typer.Exit(code=1)


@expenses_app.command("import")
def import_cmd(
    file: Optional[Path] = typer.Option(None, exists=True, dir_okay=False, help="CSV statement file"),
    dir: Optional[Path] = typer.Option(None, exists=True, file_okay=False, help="Directory of CSV statements"),
    institution: str = typer.Option(..., help="Institution label (e.g., Chase, AMEX)"),
    account: str = typer.Option(..., help="Account name label (masked; do not include full account numbers)"),
    account_type: str = typer.Option("CREDIT", help="CREDIT|BANK|UNKNOWN"),
    account_last4: str = typer.Option("", help="Last 4 digits only (stored masked)"),
    format: str = typer.Option("", help="Format override (e.g., chase_card_csv, amex_csv)"),
    fuzzy_dedupe: bool = typer.Option(True, help="Enable fuzzy duplicate detection (date±1, amount, merchant)"),
    store_original_rows: bool = typer.Option(False, help="Store redacted original row JSON (off by default)"),
):
    load_dotenv()
    _check_runtime()
    ensure_db()
    cfg, cfg_path = load_expenses_config()
    if cfg_path:
        typer.echo(f"Using config: {cfg_path}")

    files: list[Path] = []
    if file:
        files = [file]
    elif dir:
        files = sorted([p for p in dir.rglob("*.csv") if p.is_file()])
    else:
        raise typer.BadParameter("Provide --file or --dir")
    if not files:
        typer.echo("No CSV files found.", err=True)
        raise typer.Exit(code=2)

    opts = ImportOptions(
        format_name=(format.strip() or None),
        fuzzy_dedupe=bool(fuzzy_dedupe),
        store_original_rows=bool(store_original_rows),
    )
    last4 = account_last4.strip() or None

    with get_session() as session:
        for p in files:
            res = import_csv_statement(
                session=session,
                cfg=cfg,
                file_path=p,
                institution=institution.strip(),
                account_name=account.strip(),
                account_type=account_type.strip().upper(),
                account_last4=last4,
                options=opts,
            )
            typer.echo(json.dumps(res.model_dump(), indent=2))


@expenses_app.command("categorize")
def categorize_cmd(
    rebuild: bool = typer.Option(False, help="Rebuild system categories for all transactions (skips user categories)"),
    rules_path: str = typer.Option("", help="Rules YAML path override"),
):
    load_dotenv()
    _check_runtime()
    ensure_db()
    cfg, _ = load_expenses_config()
    rp = Path(rules_path.strip() or cfg.categorization.rules_path)
    if not rp.exists():
        typer.echo(f"Rules file not found: {rp}", err=True)
        raise typer.Exit(code=2)
    with get_session() as session:
        updated, skipped_user = apply_rules_to_db(
            session=session,
            rules_path=rp,
            config=cfg.categorization,
            rebuild=bool(rebuild),
        )
    typer.echo(json.dumps({"updated": updated, "skipped_user_categorized": skipped_user, "rules_path": str(rp)}, indent=2))


@expenses_app.command("recurring")
def recurring_cmd(
    year: int = typer.Option(...),
    min_months: int = typer.Option(3, help="Minimum distinct months to qualify"),
    include_income: bool = typer.Option(False, help="Include positive-amount transactions"),
):
    load_dotenv()
    _check_runtime()
    ensure_db()
    with get_session() as session:
        items = detect_recurring(session=session, year=year, min_months=min_months, include_income=include_income)
    typer.echo(json.dumps([i.model_dump() for i in items], indent=2))


@expenses_app.command("report")
def report_cmd(
    year: int = typer.Option(...),
    month: int = typer.Option(0, help="Optional month 1-12"),
    out: Optional[Path] = typer.Option(None, help="Output directory for CSV/HTML"),
):
    load_dotenv()
    _check_runtime()
    ensure_db()
    cfg, _ = load_expenses_config()
    m = int(month) if int(month) else None
    with get_session() as session:
        cat = category_summary(session=session, year=year, month=m)
        merchants = merchants_by_spend(session=session, year=year, month=m, limit=50)
        opp = opportunities(session=session, year=year, month=m)
    typer.echo("\nCategory summary\n" + format_table(cat.rows))
    from decimal import Decimal
    from src.investor.expenses.models import ReportRow

    merchant_rows = [ReportRow(key=r.merchant, spend=r.spend, income=Decimal("0"), net=-r.spend) for r in merchants]
    typer.echo("\nMerchants by spend (charges only)\n" + format_table(merchant_rows, headers=("Merchant", "Spend", "Income", "Net")))
    if opp:
        typer.echo("\nOpportunities\n" + "\n".join(f"- {x}" for x in opp))

    if m and cfg.categorization.budgets_monthly:
        b = budget_vs_actual(cat.rows, cfg.categorization.budgets_monthly)
        if b:
            # Reuse the generic table formatter by projecting to ReportRow-like shape.
            from src.investor.expenses.models import ReportRow

            budget_rows = [
                ReportRow(key=br.category, spend=br.spend, income=br.budget, net=br.over_under) for br in b
            ]
            typer.echo("\nBudgets (Income=Budget, Net=Under/Over)\n" + format_table(budget_rows))

    if out:
        out.mkdir(parents=True, exist_ok=True)
        scope = cat.scope
        write_csv(cat.rows, out / f"expenses_{scope}_categories.csv")
        write_csv(merchant_rows, out / f"expenses_{scope}_merchants.csv")
        (out / f"expenses_{scope}_categories.html").write_text(render_simple_html(f"Expenses by Category ({scope})", cat.rows))
        (out / f"expenses_{scope}_merchants.html").write_text(render_simple_html(f"Merchants ({scope})", merchant_rows))
        typer.echo(f"Wrote outputs to {out}")


@rules_app.command("init")
def rules_init_cmd(
    path: str = typer.Option("", help="Rules YAML path (defaults to config)"),
    force: bool = typer.Option(False, help="Overwrite if exists"),
):
    cfg, _ = load_expenses_config()
    rp = Path(path.strip() or cfg.categorization.rules_path)
    rp.parent.mkdir(parents=True, exist_ok=True)
    write_starter_rules(rp, config=cfg.categorization, force=bool(force))
    typer.echo(f"Wrote {rp}")


@rules_app.command("test")
def rules_test_cmd(
    file: Path = typer.Option(..., exists=True, dir_okay=False, help="CSV file to test"),
    rules_path: str = typer.Option("", help="Rules YAML path override"),
    format: str = typer.Option("", help="Format override (e.g., chase_card_csv, amex_csv)"),
    limit: int = typer.Option(30, help="Max rows to show"),
):
    cfg, _ = load_expenses_config()
    rp = Path(rules_path.strip() or cfg.categorization.rules_path)
    if not rp.exists():
        typer.echo(f"Rules file not found: {rp}", err=True)
        raise typer.Exit(code=2)
    compiled = load_rules(rp, defaults=cfg.categorization)

    content = file.read_text(encoding="utf-8-sig", errors="ignore")
    headers, rows = read_csv_rows(content)

    importer = None
    if format.strip():
        for imp in default_importers():
            if imp.format_name == format.strip():
                importer = imp
                break
        if importer is None:
            raise typer.BadParameter(f"Unknown format: {format}")
    else:
        for imp in default_importers():
            if imp.detect(headers):
                importer = imp
                break
    if importer is None:
        typer.echo(f"Could not detect importer for headers: {headers[:8]}", err=True)
        raise typer.Exit(code=2)

    raw = importer.parse_rows(rows=rows, default_currency=cfg.default_currency)
    shown = 0
    for t in raw:
        desc_norm = normalize_description(t.description)
        merchant_norm = normalize_merchant(desc_norm)
        cat, rule = categorize_one(
            merchant_norm=merchant_norm,
            description_norm=desc_norm,
            amount=float(t.amount),
            category_hint=t.category_hint,
            rules=compiled,
        )
        typer.echo(
            json.dumps(
                {
                    "posted_date": t.posted_date.isoformat(),
                    "amount": str(t.amount),
                    "merchant_norm": merchant_norm,
                    "description_norm": desc_norm,
                    "category": cat,
                    "rule": rule,
                },
                indent=2,
            )
        )
        shown += 1
        if shown >= limit:
            break
