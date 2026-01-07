from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from portfolio_report.pipeline import run_pipeline
from portfolio_report.util import parse_date

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command()
def main(
    transactions: Path = typer.Option(Path("./transactions.csv"), help="Path to transactions CSV."),
    monthly: Path = typer.Option(Path("./2025-Monthly-performance.csv"), help="Path to monthly performance CSV."),
    holdings: Optional[Path] = typer.Option(None, help="Optional path to holdings snapshot CSV."),
    out: Path = typer.Option(Path("./out")),
    start: str = typer.Option("2025-01-01", help="Start date (YYYY-MM-DD)."),
    end: str = typer.Option("2025-12-31", help="End date (YYYY-MM-DD)."),
    asof: Optional[str] = typer.Option(None, help="Month-end to generate a single-month report (YYYY-MM-DD)."),
    benchmark: str = typer.Option("SPY", help="Benchmark ticker (default SPY)."),
    prices_dir: Path = typer.Option(Path("./data/prices"), help="Local price cache folder."),
    download_prices: bool = typer.Option(False, help="Fetch missing prices via yfinance (requires internet + yfinance)."),
    include_fees_as_flow: bool = typer.Option(False, help="Treat fees as external flows (gross-of-fees returns)."),
):
    """
    Generate monthly performance report(s), analytics marts, and position guidance.
    """
    if not transactions.exists():
        raise typer.BadParameter(f"Transactions file not found: {transactions}")
    if not monthly.exists():
        raise typer.BadParameter(f"Monthly performance file not found: {monthly}")
    if holdings is not None and not holdings.exists():
        raise typer.BadParameter(f"Holdings file not found: {holdings}")
    start_d = parse_date(start)
    end_d = parse_date(end)
    asof_d = parse_date(asof) if asof is not None else None
    if start_d is None:
        raise typer.BadParameter(f"Invalid --start date: {start}")
    if end_d is None:
        raise typer.BadParameter(f"Invalid --end date: {end}")
    if asof is not None and asof_d is None:
        raise typer.BadParameter(f"Invalid --asof date: {asof}")
    out.mkdir(parents=True, exist_ok=True)
    prices_dir.mkdir(parents=True, exist_ok=True)
    run_pipeline(
        transactions_csv=transactions,
        monthly_perf_csv=monthly,
        holdings_csv=holdings,
        out_dir=out,
        prices_dir=prices_dir,
        start_date=start_d,
        end_date=end_d,
        asof_date=asof_d,
        benchmark_symbol=benchmark,
        download_prices=download_prices,
        include_fees_as_flow=include_fees_as_flow,
    )
