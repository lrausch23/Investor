from __future__ import annotations

import datetime as dt
from pathlib import Path

from portfolio_report.pipeline import run_pipeline


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_pipeline_smoke(tmp_path: Path):
    tx = tmp_path / "transactions.csv"
    monthly = tmp_path / "2025-Monthly-performance.csv"
    prices_dir = tmp_path / "data" / "prices"
    out_dir = tmp_path / "out"
    prices_dir.mkdir(parents=True, exist_ok=True)

    _write(
        tx,
        "Date,Symbol,Type,Quantity,Price,Amount\n"
        "2025-01-10,AAA,BUY,10,10,-100\n"
        "2025-06-10,AAA,SELL,5,12,60\n"
        "2025-03-15,,BNK,,,-1000\n"
        "2025-09-15,,BNK,,,500\n",
    )
    _write(
        monthly,
        "Date,Beginning market value,Ending market value,Contributions,Withdrawals,Taxes withheld,Fees,Income\n"
        "2025-01-31,10000,10200,0,0,0,0,0\n"
        "2025-02-28,10200,10100,0,0,0,0,0\n"
        "2025-03-31,10100,11100,1000,0,0,0,0\n"
        "2025-04-30,11100,11200,0,0,0,0,0\n"
        "2025-05-31,11200,11300,0,0,0,0,0\n"
        "2025-06-30,11300,11400,0,0,0,0,0\n"
        "2025-07-31,11400,11500,0,0,0,0,0\n"
        "2025-08-31,11500,11600,0,0,0,0,0\n"
        "2025-09-30,11600,11100,0,500,0,0,0\n"
        "2025-10-31,11100,11200,0,0,0,0,0\n"
        "2025-11-30,11200,11300,0,0,0,0,0\n"
        "2025-12-31,11300,12000,0,0,0,0,0\n",
    )
    _write(
        prices_dir / "SPY.csv",
        "Date,Adj Close\n"
        "2025-01-31,100\n"
        "2025-02-28,101\n"
        "2025-03-31,102\n"
        "2025-04-30,103\n"
        "2025-05-31,104\n"
        "2025-06-30,105\n"
        "2025-07-31,106\n"
        "2025-08-31,107\n"
        "2025-09-30,108\n"
        "2025-10-31,109\n"
        "2025-11-30,110\n"
        "2025-12-31,111\n",
    )
    _write(
        prices_dir / "AAA.csv",
        "Date,Adj Close\n"
        "2025-01-02,10\n"
        "2025-01-31,11\n"
        "2025-06-30,12\n"
        "2025-12-31,13\n",
    )

    run_pipeline(
        transactions_csv=tx,
        monthly_perf_csv=monthly,
        holdings_csv=None,
        out_dir=out_dir,
        prices_dir=prices_dir,
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        asof_date=dt.date(2025, 12, 31),
        benchmark_symbol="SPY",
        download_prices=False,
        include_fees_as_flow=False,
    )

    assert (out_dir / "report_2025-12.html").exists()
    assert (out_dir / "report_full_year_2025.html").exists()
    assert (out_dir / "analytics_monthly.csv").exists()
    assert (out_dir / "analytics_daily.csv").exists()
    assert (out_dir / "position_guidance_2025-12.csv").exists()
    assert (out_dir / "run_metadata.json").exists()

