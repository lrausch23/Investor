from __future__ import annotations

import csv
import datetime as dt
import json
from dataclasses import asdict
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from portfolio_report.fifo import fifo_realized_pnl
from portfolio_report.holdings import load_holdings
from portfolio_report.monthly_perf import load_monthly_perf
from portfolio_report.prices import daily_returns, load_prices
from portfolio_report.returns import chain_link, modified_dietz_return, risk_stats_from_returns, xirr
from portfolio_report.transactions import load_transactions, transactions_by_symbol
from portfolio_report.util import month_ends, uniq_sorted


def _try_write_parquet_or_csv(rows: list[dict], *, parquet_path: Path, csv_path: Path) -> list[str]:
    """
    Writes Parquet if possible (pandas+pyarrow or pyarrow), otherwise writes CSV.
    Always writes CSV as a fallback artifact.
    """
    warnings: list[str] = []
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    # CSV always (deterministic, dependency-free).
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
        else:
            cols = sorted({k for r in rows for k in r.keys()})
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k) for k in cols})

    # Parquet best-effort (optional dependency).
    try:
        import pandas as pd  # type: ignore

        df = pd.DataFrame(rows)
        df.to_parquet(parquet_path, index=False)
        return warnings
    except Exception:
        pass
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        table = pa.Table.from_pylist(rows)
        pq.write_table(table, parquet_path)
        return warnings
    except Exception as e:
        # Avoid surfacing missing optional deps as a "data sufficiency" warning in the report.
        if isinstance(e, (ImportError, ModuleNotFoundError)):
            return warnings
        warnings.append(f"Parquet not written ({parquet_path.name}): {e}. CSV written to {csv_path.name} instead.")
        return warnings


def run_pipeline(
    *,
    transactions_csv: Path,
    monthly_perf_csv: Path,
    holdings_csv: Path | None,
    out_dir: Path,
    prices_dir: Path,
    start_date: dt.date,
    end_date: dt.date,
    asof_date: dt.date | None,
    benchmark_symbol: str,
    download_prices: bool,
    include_fees_as_flow: bool,
) -> None:
    warnings: list[str] = []

    txs, tx_warn = load_transactions(transactions_csv)
    warnings.extend(tx_warn)
    tx_by_sym = transactions_by_symbol(txs)

    monthly_rows, mon_warn = load_monthly_perf(monthly_perf_csv, start=start_date, end=end_date)
    warnings.extend(mon_warn)

    holdings = []
    if holdings_csv is not None:
        holdings, hold_warn = load_holdings(holdings_csv)
        warnings.extend(hold_warn)

    # Resolve month ends for reporting.
    mes = month_ends(start_date, end_date)
    if not mes:
        raise SystemExit("No month-ends in selected range.")

    # Portfolio begin/end values (from monthly CSV).
    monthly_by_me = {r.month_end: r for r in monthly_rows}
    missing_me = [d for d in mes if d not in monthly_by_me]
    if missing_me:
        warnings.append(f"Monthly performance is missing {len(missing_me)} month(s): {', '.join(d.isoformat() for d in missing_me[:6])}{'...' if len(missing_me)>6 else ''}")

    # Symbols universe for pricing: restrict to holdings + traded symbols (BUY/SELL) + benchmark.
    # This avoids attempting to price cashflow-only pseudo symbols (e.g., internal sweeps).
    traded_symbols = {t.symbol for t in txs if t.symbol and t.tx_type in {"BUY", "SELL"} and t.symbol not in {"UNKNOWN"}}
    symbols = uniq_sorted(list(traded_symbols) + [h.symbol for h in holdings] + [benchmark_symbol])

    prices, price_warn = load_prices(symbols=symbols, prices_dir=prices_dir, start=start_date, end=end_date, download=download_prices)
    warnings.extend(price_warn)

    # Benchmark monthly returns.
    bench = prices.get(benchmark_symbol)
    bench_monthly = bench.returns_by_month_end(mes) if bench is not None else {}

    # Portfolio monthly returns (Dietz), and chain-linked TWR.
    monthly_analytics: list[dict] = []
    dietz_rets: list[float] = []
    dietz_ret_by_me: dict[dt.date, float] = {}
    for me in mes:
        r = monthly_by_me.get(me)
        if r is None or r.begin_value is None or r.end_value is None:
            continue
        net_flow = r.net_external_flow_portfolio
        if include_fees_as_flow:
            net_flow -= float(r.fees or 0.0)
        ret = modified_dietz_return(begin_value=float(r.begin_value), end_value=float(r.end_value), net_external_flow=float(net_flow), flow_weight=0.5)
        if ret is None:
            continue
        dietz_rets.append(float(ret))
        dietz_ret_by_me[me] = float(ret)
        monthly_analytics.append(
            {
                "month_end": me.isoformat(),
                "begin_value": float(r.begin_value),
                "end_value": float(r.end_value),
                "contributions": float(r.contributions),
                "withdrawals": float(r.withdrawals),
                "taxes_withheld": float(r.taxes_withheld),
                "fees": float(r.fees),
                "income": float(r.income),
                "net_external_flow_portfolio": float(net_flow),
                "dietz_return": float(ret),
                "benchmark_return": float(bench_monthly.get(me)) if me in bench_monthly else None,
                "excess_return": (float(ret) - float(bench_monthly.get(me))) if me in bench_monthly else None,
            }
        )
    twr_ytd = chain_link(dietz_rets)
    bench_ytd = chain_link([bench_monthly[m] for m in mes if m in bench_monthly]) if bench_monthly else None
    risk = risk_stats_from_returns(portfolio_returns=dietz_rets, benchmark_returns=[bench_monthly[m] for m in mes if m in bench_monthly] if bench_monthly else None, periods_per_year=12.0)

    # XIRR cashflows (investor perspective).
    # Prefer dated external flows from transactions (BNK/TAX). If absent, approximate with mid-month net flows from monthly table.
    irr_warn: list[str] = []
    cfs: list[tuple[dt.date, float]] = []
    # Start value:
    first_me = mes[0]
    first_row = monthly_by_me.get(first_me)
    if first_row and first_row.begin_value is not None:
        cfs.append((start_date, -float(first_row.begin_value)))
    else:
        irr_warn.append("Missing begin NAV for XIRR; cannot compute.")
    # External flows:
    ext_txs = [t for t in txs if t.is_external and t.external_cashflow_investor is not None and start_date <= t.date <= end_date]
    if ext_txs:
        for t in ext_txs:
            cfs.append((t.date, float(t.external_cashflow_investor or 0.0)))
    else:
        irr_warn.append("No dated external cashflows found in transactions; using mid-month approximation from monthly performance.")
        for me in mes:
            r = monthly_by_me.get(me)
            if r is None:
                continue
            net_flow = r.net_external_flow_portfolio
            if include_fees_as_flow:
                net_flow -= float(r.fees or 0.0)
            # Investor cashflow is negative of portfolio cashflow.
            inv_flow = -float(net_flow)
            if abs(inv_flow) <= 1e-9:
                continue
            mid = dt.date(me.year, me.month, 15)
            cfs.append((mid, inv_flow))
    # End value:
    last_me = mes[-1]
    last_row = monthly_by_me.get(last_me)
    if last_row and last_row.end_value is not None:
        cfs.append((end_date, float(last_row.end_value)))

    irr = xirr(cfs) if len(cfs) >= 2 else None
    if irr is None:
        irr_warn.append("XIRR could not be solved (cashflows may not include both positive and negative values).")

    # Realized P&L (FIFO), monthly aggregates.
    realized_rows: list[dict] = []
    realized_warn: list[str] = []
    for sym in sorted(traded_symbols):
        matches, w = fifo_realized_pnl(txs, symbol=sym)
        realized_warn.extend(w)
        for m in matches:
            realized_rows.append(
                {
                    "symbol": m.symbol,
                    "sell_date": m.sell_date.isoformat(),
                    "qty": m.qty,
                    "proceeds": m.proceeds,
                    "cost": m.cost,
                    "pnl": m.pnl,
                    "carry_in_basis_unknown": bool(m.carry_in_basis_unknown),
                }
            )

    # Holdings reconstruction (qty over time) for contribution approximation.
    # If sells exceed in-period buys, infer a starting (carry-in) position so quantities don't go negative.
    qty_by_sym_date: dict[tuple[str, dt.date], float] = {}
    seed_qty_by_sym: dict[str, float] = {}
    for sym in sorted(traded_symbols):
        running = 0.0
        min_running = 0.0
        by_date: list[tuple[dt.date, float]] = []
        for t in tx_by_sym.get(sym, []):
            if t.tx_type not in {"BUY", "SELL"}:
                continue
            q = float(t.qty or 0.0)
            if q == 0:
                continue
            running += abs(q) if t.tx_type == "BUY" else -abs(q)
            min_running = min(min_running, running)
            by_date.append((t.date, running))
        seed = -min_running if min_running < -1e-9 else 0.0
        if seed > 0:
            seed_qty_by_sym[sym] = seed
        for d, q in by_date:
            qty_by_sym_date[(sym, d)] = q + seed

    # Month-level position contributions (approx avg weight × return + dividend yield contribution).
    contrib_rows: list[dict] = []
    contrib_warn: list[str] = []
    for sym in sorted(traded_symbols):
        ps = prices.get(sym)
        if ps is None or not ps.points:
            continue
        seed = float(seed_qty_by_sym.get(sym, 0.0))
        if seed > 0:
            contrib_warn.append(f"{sym}: inferred starting position of {seed:.6g} shares (carry-in holdings); contribution is approximate.")
        for me in mes:
            r = monthly_by_me.get(me)
            if r is None or r.begin_value is None or r.end_value is None:
                continue
            # Month start approx: first day of month.
            ms = dt.date(me.year, me.month, 1)
            p0 = ps.price_on_or_before(ms)
            p1 = ps.price_on_or_before(me)
            if p0 is None or p1 is None or p0 <= 0:
                continue
            ret_i = float(p1 / p0 - 1.0)

            # Quantity at start/end (approx using last transaction qty before date).
            def qty_on(d: dt.date) -> float:
                # carry forward last known qty.
                cand = [dd for (s, dd) in qty_by_sym_date.keys() if s == sym and dd <= d]
                if not cand:
                    return float(seed_qty_by_sym.get(sym, 0.0))
                last_d = max(cand)
                return float(qty_by_sym_date[(sym, last_d)])

            q0 = qty_on(ms)
            q1 = qty_on(me)
            qavg = 0.5 * (q0 + q1)
            pxavg = 0.5 * (float(p0) + float(p1))
            mv_avg = float(qavg) * float(pxavg)
            port_avg = 0.5 * (float(r.begin_value) + float(r.end_value))
            if port_avg <= 0:
                continue
            wavg = mv_avg / port_avg
            contrib = wavg * ret_i
            contrib_rows.append(
                {
                    "month_end": me.isoformat(),
                    "symbol": sym,
                    "avg_weight": wavg,
                    "price_return": ret_i,
                    "price_contribution": contrib,
                }
            )

    # Daily analytics mart (best-effort): benchmark trading dates as the spine.
    daily_rows: list[dict] = []
    if bench is not None and bench.points:
        bench_rets = daily_returns(bench.points)
        for d, px in bench.points:
            if d < start_date or d > end_date:
                continue
            daily_rows.append({"date": d.isoformat(), "symbol": benchmark_symbol, "close": px, "ret_1d": bench_rets.get(d)})
    else:
        warnings.append("No benchmark price series; daily analytics mart will be empty.")

    # Write marts.
    out_dir.mkdir(parents=True, exist_ok=True)
    mart_warn = []
    mart_warn.extend(_try_write_parquet_or_csv(monthly_analytics, parquet_path=out_dir / "analytics_monthly.parquet", csv_path=out_dir / "analytics_monthly.csv"))
    mart_warn.extend(_try_write_parquet_or_csv(daily_rows, parquet_path=out_dir / "analytics_daily.parquet", csv_path=out_dir / "analytics_daily.csv"))
    warnings.extend(mart_warn)

    # Guidance (as-of).
    asof = asof_date or end_date
    guidance_rows: list[dict] = []
    # Determine holdings universe for guidance:
    if holdings:
        hold_map = {h.symbol: h for h in holdings}
        held_syms = sorted(hold_map.keys())
    else:
        # Active symbols traded in trailing 3 months.
        cutoff = asof - dt.timedelta(days=92)
        held_syms = sorted({t.symbol for t in txs if t.symbol and t.date >= cutoff})
        hold_map = {}
    # Contribution YTD:
    ytd_contrib: dict[str, float] = {}
    last_me_asof = dt.date(asof.year, asof.month, 1)
    # pick month end <= asof
    asof_mes = [d for d in mes if d <= asof]
    last_me = max(asof_mes) if asof_mes else mes[-1]
    last_month_contrib: dict[str, float] = {}
    for r in contrib_rows:
        sym = r["symbol"]
        me = dt.date.fromisoformat(r["month_end"])
        ytd_contrib[sym] = float(ytd_contrib.get(sym, 0.0)) + float(r["price_contribution"] or 0.0)
        if me == last_me:
            last_month_contrib[sym] = float(last_month_contrib.get(sym, 0.0)) + float(r["price_contribution"] or 0.0)

    # Realized P&L YTD:
    ytd_realized: dict[str, float] = {}
    carry_in_flag: dict[str, bool] = {}
    for rr in realized_rows:
        sym = rr["symbol"]
        d = dt.date.fromisoformat(rr["sell_date"])
        if start_date <= d <= asof:
            if rr.get("pnl") is not None:
                ytd_realized[sym] = float(ytd_realized.get(sym, 0.0)) + float(rr["pnl"] or 0.0)
            if rr.get("carry_in_basis_unknown"):
                carry_in_flag[sym] = True

    # Market values and risk from prices.
    total_mv = 0.0
    mv_by_sym: dict[str, float] = {}
    for sym in held_syms:
        ps = prices.get(sym)
        px = ps.price_on_or_before(asof) if ps else None
        h = hold_map.get(sym) if sym in hold_map else None
        qty = float(h.quantity) if h is not None else None
        if qty is None:
            # reconstruct qty at asof
            q = 0.0
            for t in tx_by_sym.get(sym, []):
                if t.date > asof:
                    break
                if t.tx_type == "BUY":
                    q += abs(float(t.qty or 0.0))
                elif t.tx_type == "SELL":
                    q -= abs(float(t.qty or 0.0))
            qty = q + float(seed_qty_by_sym.get(sym, 0.0))
        # Prefer market_value from holdings snapshot when price history is missing.
        if px is None or float(px or 0.0) <= 0:
            mv = float(h.market_value) if (h is not None and h.market_value is not None) else 0.0
        else:
            mv = float(qty or 0.0) * float(px)
        mv_by_sym[sym] = mv
        total_mv += mv

    # Simple guidance rules (deterministic).
    for sym in held_syms:
        mv = float(mv_by_sym.get(sym, 0.0))
        weight = (mv / total_mv) if total_mv > 0 else 0.0
        contrib_ytd = float(ytd_contrib.get(sym, 0.0))
        contrib_last = float(last_month_contrib.get(sym, 0.0))
        realized = float(ytd_realized.get(sym, 0.0))

        # Risk stats from monthly benchmark-aligned returns if possible.
        ps = prices.get(sym)
        # If we have no price history and effectively no exposure, omit from guidance to keep output actionable.
        if (ps is None or not ps.points) and weight <= 1e-12 and abs(realized) <= 1e-9:
            continue
        sym_monthly = ps.returns_by_month_end(mes) if ps is not None else {}
        sym_rets = [sym_monthly[m] for m in mes if m in sym_monthly]
        bench_rets = [bench_monthly[m] for m in mes if m in bench_monthly] if bench_monthly else None
        rs = risk_stats_from_returns(portfolio_returns=sym_rets, benchmark_returns=bench_rets, periods_per_year=12.0)

        # Action rules.
        action = "WATCH"
        rationale: list[str] = []
        if weight >= 0.15:
            rationale.append(f"Concentration {weight*100:.1f}% ≥ 15%.")
            if rs.vol is not None and rs.vol >= 0.30:
                action = "TRIM"
                rationale.append(f"Volatility {rs.vol*100:.0f}% is high.")
            else:
                action = "WATCH"
        if action == "WATCH" and contrib_ytd > 0.01:
            action = "HOLD"
            rationale.append("Positive YTD contribution.")
        if contrib_ytd < -0.01 and realized < 0:
            action = "EXIT" if weight >= 0.05 else "WATCH"
            rationale.append("Negative YTD contribution and negative realized P&L.")
        if action in {"HOLD", "WATCH"} and weight <= 0.03 and contrib_ytd > 0.005 and contrib_last > 0:
            action = "ADD"
            rationale.append("Low weight with recent positive contribution.")

        if carry_in_flag.get(sym):
            rationale.append("Some sells have unknown cost basis; realized P&L excludes those fills.")
        if ps is None or not ps.points:
            rationale.append("Missing price history; risk stats limited.")

        guidance_rows.append(
            {
                "asof": asof.isoformat(),
                "symbol": sym,
                "market_value": mv,
                "weight": weight,
                "ytd_price_contribution": contrib_ytd,
                "last_month_price_contribution": contrib_last,
                "ytd_realized_pnl": realized,
                "vol_annual": rs.vol,
                "beta": rs.beta,
                "corr": rs.corr,
                "action": action,
                "rationale": " ".join(rationale) if rationale else "Insufficient data for stronger signal.",
            }
        )

    # Write guidance.
    guidance_path = out_dir / f"position_guidance_{asof.year}-{asof.month:02d}.csv"
    with guidance_path.open("w", newline="", encoding="utf-8") as f:
        cols = sorted({k for r in guidance_rows for k in r.keys()})
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in sorted(guidance_rows, key=lambda x: (-float(x.get("weight") or 0.0), str(x.get("symbol") or ""))):
            w.writerow({k: r.get(k) for k in cols})

    # Render HTML reports.
    env = Environment(
        loader=FileSystemLoader(str(Path(__file__).resolve().parent / "templates")),
        autoescape=select_autoescape(["html"]),
    )
    tpl = env.get_template("report.html")

    # Helpers for charts (simple inline SVG).
    def bar_chart_svg(points: list[tuple[str, float | None]], *, height: int = 120, width: int = 640) -> str:
        vals = [p[1] for p in points if p[1] is not None]
        if not vals:
            return "<div class='muted'>No chart data.</div>"
        mx = max(abs(float(v)) for v in vals) or 1.0
        n = len(points)
        bar_w = max(2, int(width / max(1, n)))
        mid_y = height // 2
        svg = [f"<svg viewBox='0 0 {width} {height}' width='{width}' height='{height}'>"]
        svg.append(f"<line x1='0' y1='{mid_y}' x2='{width}' y2='{mid_y}' stroke='#ccc' stroke-width='1' />")
        for i, (label, v) in enumerate(points):
            if v is None:
                continue
            x = i * bar_w
            h = int((abs(float(v)) / mx) * (height * 0.45))
            if float(v) >= 0:
                y = mid_y - h
                color = "#2c7"
            else:
                y = mid_y
                color = "#d55"
            svg.append(f"<rect x='{x}' y='{y}' width='{bar_w-1}' height='{h}' fill='{color}' opacity='0.85' />")
        svg.append("</svg>")
        return "".join(svg)

    # Month labels.
    month_labels = [(dt.date.fromisoformat(r["month_end"]).strftime("%Y-%m"), r.get("dietz_return")) for r in monthly_analytics]
    bench_labels = [(dt.date.fromisoformat(r["month_end"]).strftime("%Y-%m"), r.get("benchmark_return")) for r in monthly_analytics]

    ctx = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "asof": asof.isoformat(),
        "benchmark": benchmark_symbol,
        "twr_ytd": twr_ytd,
        "irr": irr,
        "bench_ytd": bench_ytd,
        "risk": asdict(risk),
        "monthly": monthly_analytics,
        "warnings": warnings + irr_warn + realized_warn + contrib_warn,
        "chart_portfolio": bar_chart_svg(month_labels),
        "chart_benchmark": bar_chart_svg(bench_labels),
        "guidance_top": sorted(guidance_rows, key=lambda r: (-float(r.get("weight") or 0.0), str(r.get("symbol") or "")))[:15],
        "realized_top": sorted([r for r in realized_rows if r.get("pnl") is not None], key=lambda r: -float(r.get("pnl") or 0.0))[:15],
        "realized_bottom": sorted([r for r in realized_rows if r.get("pnl") is not None], key=lambda r: float(r.get("pnl") or 0.0))[:15],
        "realized_basis_unknown": sorted([r for r in realized_rows if r.get("pnl") is None], key=lambda r: -float(r.get("proceeds") or 0.0))[:25],
    }

    # Single-month report for `asof` month.
    report_month_path = out_dir / f"report_{asof.year}-{asof.month:02d}.html"
    report_month_path.write_text(tpl.render(title=f"Portfolio Report {asof.year}-{asof.month:02d}", **ctx), encoding="utf-8")

    # Full-year report.
    report_year_path = out_dir / f"report_full_year_{start_date.year}.html"
    report_year_path.write_text(tpl.render(title=f"Portfolio Report Full Year {start_date.year}", **ctx), encoding="utf-8")

    # Diagnostics bundle (inputs + summary) for reproducibility.
    (out_dir / "run_metadata.json").write_text(
        json.dumps(
            {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "asof": asof.isoformat(),
                "benchmark": benchmark_symbol,
                "download_prices": bool(download_prices),
                "include_fees_as_flow": bool(include_fees_as_flow),
                "inputs": {
                    "transactions_csv": str(transactions_csv),
                    "monthly_perf_csv": str(monthly_perf_csv),
                    "holdings_csv": str(holdings_csv) if holdings_csv else None,
                    "prices_dir": str(prices_dir),
                },
                "summary": {
                    "twr_ytd": twr_ytd,
                    "xirr": irr,
                    "bench_ytd": bench_ytd,
                    "warnings_count": len(ctx["warnings"]),
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
