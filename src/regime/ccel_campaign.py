from __future__ import annotations

import datetime as dt
import html
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd

from .alpha_campaign import DEFAULT_BASKET_PATH, DEFAULT_CAMPAIGN_DIR, _git_sha, _json_safe, _read_json, _write_json, load_basket
from .data import download_market_frame
from .pipeline_backtest import compute_equity_metrics
from .portfolio_backtest import PortfolioBacktestConfig, run_portfolio_backtest
from .portfolio_campaign import _campaign_row, _enrich_regime_frames as _campaign2_enrich_regime_frames
from .portfolio_campaign import _float, _fmt_num, _fmt_pct, _safe_name, campaign2_headline_specs
from .portfolio_campaign3 import campaign3_specs, with_market_timing_signal
from .portfolio_historical_campaign import (
    FrameLoader,
    _benchmark_relative_rows,
    _buy_hold_curve,
    _html_table,
    _limitations,
    _load_historical_frame,
    _period_returns,
    _slice_frame,
    _stress_results_for_curve,
    build_availability_report,
    historical_stress_windows_for_range,
)
from .sharadar import DEFAULT_SHARADAR_DIR, SharadarFrameLoader, certification_gate_status, classify_readiness
from .stress_windows import StressWindow
from .strategy import StrategySpec

DEFAULT_CCEL_START = "2006-01-01"
DEFAULT_CCEL_END = "2025-12-31"
DEFAULT_CCEL_OOS_START = "2024-01-01"
DEFAULT_CCEL_CAMPAIGN_DIR = DEFAULT_CAMPAIGN_DIR / "ccel_v1a_2006_2025"
DEFAULT_CCEL_REPORT_DIR = Path("output") / "ccel_v1a_2006_2025_report"
DEFAULT_CCEL_REPORT_PATH = DEFAULT_CCEL_REPORT_DIR / "management_report.html"
PROXY_LABEL = "NOT survivorship-free - exploratory proxy"


@dataclass(frozen=True)
class CCELConfig:
    starting_cash: float = 100_000.0
    entry_cost_bps: float = 5.0
    exit_cost_bps: float = 5.0
    st_tax_rate: float = 0.32
    lt_tax_rate: float = 0.20
    oos_start: str = DEFAULT_CCEL_OOS_START
    significant_gain_pct: float = 0.10
    harvest_loss_pct: float = 0.08
    probation_loss_pct: float = 0.15
    probation_days: int = 365
    min_harvest_age_days: int = 31
    no_rebuy_days: int = 31
    max_names: int = 35
    min_cash_to_deploy: float = 250.0
    integer_shares: bool = True
    harvest_enabled: bool = True
    harvest_replacement_mode: Literal["exposure_neutral", "momentum"] = "exposure_neutral"
    harvest_reentry: bool = True
    probation_enabled: bool = True
    core_only: bool = False
    momentum_breakdown_enabled: bool = True
    momentum_bottom_quantile: float = 0.25

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CCELLot:
    lot_id: int
    ticker: str
    quantity: float
    basis_per_share: float
    acquisition_date: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PendingInstruction:
    decision_date: str
    sell_tickers: dict[str, str] = field(default_factory=dict)
    buy_candidates: list[str] = field(default_factory=list)
    harvest_replacement_tickers: list[str] = field(default_factory=list)
    harvest_reentry_targets: dict[str, float] = field(default_factory=dict)
    reason: str = ""


def run_ccel_campaign(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CCEL_CAMPAIGN_DIR,
    report_dir: str | Path = DEFAULT_CCEL_REPORT_DIR,
    start: str = DEFAULT_CCEL_START,
    end: str = DEFAULT_CCEL_END,
    oos_start: str = DEFAULT_CCEL_OOS_START,
    resume: bool = False,
    frame_loader: FrameLoader | None = None,
    data_source: Literal["proxy", "sharadar"] = "proxy",
    sharadar_store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    regime_enricher: Callable[[dict[str, pd.DataFrame]], dict[str, pd.DataFrame]] | None = None,
    render_report: bool = True,
) -> dict[str, Any]:
    started = dt.datetime.now(dt.timezone.utc)
    root = Path(campaign_dir)
    result_dir = root / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    basket = load_basket(basket_path)
    tickers = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    if not tickers:
        raise ValueError("CCEL campaign requires a pinned campaign basket.")

    source = str(data_source or "proxy").lower()
    if source not in {"proxy", "sharadar"}:
        raise ValueError("data_source must be 'proxy' or 'sharadar'.")
    loader = frame_loader or (SharadarFrameLoader(sharadar_store_dir) if source == "sharadar" else _load_historical_frame)
    raw_frames: dict[str, pd.DataFrame] = {}
    load_errors: dict[str, str] = {}
    for ticker in tickers:
        try:
            frame = _slice_frame(loader(ticker, start, end), start, end)
        except Exception as exc:
            load_errors[ticker] = str(exc)
            continue
        if frame.empty:
            load_errors[ticker] = "empty adjusted-price frame for requested window"
            continue
        raw_frames[ticker] = frame
    if not raw_frames:
        raise ValueError("No basket constituents have usable CCEL history.")

    benchmarks: dict[str, pd.DataFrame] = {}
    for ticker in ("SPY", "QQQ"):
        try:
            frame = _slice_frame(loader(ticker, start, end), start, end)
        except Exception as exc:
            load_errors[ticker] = str(exc)
            frame = pd.DataFrame()
        if frame.empty:
            load_errors[ticker] = load_errors.get(ticker, "empty benchmark history")
        benchmarks[ticker] = frame
    if benchmarks["SPY"].empty:
        raise ValueError("SPY benchmark history is required for CCEL v1a.")
    if benchmarks["QQQ"].empty:
        raise ValueError("QQQ benchmark history is required for CCEL v1a.")

    readiness, snapshot_hash = _data_layer_status(
        data_source=source,
        loader=loader,
        tickers=[*tickers, "SPY", "QQQ"],
        start=start,
        end=end,
    )
    stress_windows = historical_stress_windows_for_range(start, end)
    benchmark_curve = _buy_hold_curve(benchmarks["SPY"], starting_cash=100_000.0)
    results: dict[str, dict[str, Any]] = {}
    ccel_arms = ccel_arm_configs(oos_start=oos_start)
    all_result_frames = {**raw_frames, "SPY": benchmarks["SPY"], "QQQ": benchmarks["QQQ"]}

    for arm, cfg in ccel_arms.items():
        path = result_dir / f"{_safe_name(arm)}.json"
        if resume and path.exists():
            payload = _stamp_data_layer(dict(_read_json(path)), data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
            _write_json(path, payload)
            results[arm] = payload
            continue
        payload = run_ccel_backtest(raw_frames, cfg, benchmark_curve=benchmark_curve, windows=stress_windows)
        payload["campaign"] = _campaign_metadata(arm=arm, start=start, end=end, oos_start=oos_start, kind="ccel_proxy")
        payload = _stamp_data_layer(payload, data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
        _write_json(path, payload)
        results[arm] = payload

    for arm, payload in _run_reference_arms(
        raw_frames=raw_frames,
        benchmarks=benchmarks,
        start=start,
        end=end,
        oos_start=oos_start,
        windows=stress_windows,
        benchmark_curve=benchmark_curve,
        regime_enricher=regime_enricher,
    ).items():
        path = result_dir / f"{_safe_name(arm)}.json"
        if resume and path.exists():
            payload = _stamp_data_layer(dict(_read_json(path)), data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
            _write_json(path, payload)
            results[arm] = payload
            continue
        payload["campaign"] = _campaign_metadata(arm=arm, start=start, end=end, oos_start=oos_start, kind="reference")
        payload = _stamp_data_layer(payload, data_source=source, readiness=readiness, snapshot_hash=snapshot_hash)
        _write_json(path, payload)
        results[arm] = payload

    rows = [
        _ccel_row(arm, payload, result_dir / f"{_safe_name(arm)}.json")
        for arm, payload in results.items()
    ]
    rows = sorted(rows, key=_row_order)
    verdict = ccel_verdict(rows)
    verdict["data_readiness"] = readiness.get("data_readiness")
    verdict["certification_gate_status"] = certification_gate_status(
        str(readiness.get("data_readiness") or "price_only_proxy"),
        after_tax=True,
        out_of_sample=True,
        killed=not bool(verdict.get("ccel_v1a_not_killed")),
    )
    monthly_returns = {arm: _period_returns(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [], "M") for arm, payload in results.items()}
    yearly_returns = {arm: _period_returns(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [], "Y") for arm, payload in results.items()}
    finished = dt.datetime.now(dt.timezone.utc)
    coverage = build_availability_report(raw_frames, start=start, end=end, load_errors=load_errors, basket=basket)
    summary = {
        "schema": "regime_ccel_v1a_campaign.v1",
        "generated_at": finished.isoformat(),
        "git_sha": _git_sha(),
        "wall_clock_seconds": (finished - started).total_seconds(),
        "basket_path": str(basket_path),
        "basket_size": len(tickers),
        "available_ticker_count": len(raw_frames),
        "tickers": tickers,
        "available_tickers": sorted(raw_frames),
        "unavailable_tickers": sorted(load_errors),
        "start": start,
        "end": end,
        "oos_start": oos_start,
        "availability_mode": "panel",
        "data_source": source,
        "data_readiness": readiness.get("data_readiness"),
        "readiness": readiness,
        "data_snapshot_hash": snapshot_hash,
        "proxy_label": PROXY_LABEL,
        "adjusted_price_note": _adjusted_price_note(source),
        "rows": rows,
        "benchmark_relative": _benchmark_relative_rows(rows),
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "coverage": coverage,
        "stress_windows": [window.to_dict() for window in stress_windows],
        "verdict": verdict,
        "single_command": (
            "python -m src.regime.cli ccel-campaign run "
            f"--start {start} --end {end} --oos-start {oos_start} --campaign-dir {root} --report-dir {Path(report_dir)} "
            f"{_data_source_cli_args(source, sharadar_store_dir)}--resume"
        ),
        "production_defaults_changed": False,
        "limitations": ccel_limitations(start=start, end=end, load_errors=load_errors) + _data_layer_limitations(source, readiness),
    }
    _write_json(root / "summary.json", summary)
    _write_json(
        root / "cache_manifest.json",
        {
            "schema": "regime_ccel_v1a_cache.v1",
            "generated_at": started.isoformat(),
            "git_sha": _git_sha(),
            "start": start,
            "end": end,
            "oos_start": oos_start,
            "tickers": tickers,
            "available_tickers": sorted(raw_frames),
            "benchmarks": ["SPY", "QQQ"],
            "data_source": source,
            "data_readiness": readiness.get("data_readiness"),
            "readiness": readiness,
            "data_snapshot_hash": snapshot_hash,
            "proxy_label": PROXY_LABEL,
            "frozen_cache_note": "Frames are loaded once at campaign start and sliced to the requested window.",
        },
    )
    _append_run_log(root, f"CCEL v1a campaign completed in {summary['wall_clock_seconds']:.1f}s with {len(results)} arms.")
    if render_report:
        report_path = render_ccel_report(campaign_dir=root, output_dir=report_dir)
        summary["report_path"] = str(report_path)
        _write_json(root / "summary.json", summary)
    return summary


def ccel_arm_configs(*, oos_start: str = DEFAULT_CCEL_OOS_START) -> dict[str, CCELConfig]:
    base = CCELConfig(oos_start=oos_start)
    return {
        "CCEL_v1a": base,
        "CCEL_no_harvest": _replace_config(base, harvest_enabled=False),
        "CCEL_core_only": _replace_config(base, core_only=True, probation_enabled=False),
        "CCEL_core_only_no_harvest": _replace_config(base, core_only=True, probation_enabled=False, harvest_enabled=False),
    }


def run_ccel_backtest(
    market_frames: dict[str, pd.DataFrame],
    config: CCELConfig | None = None,
    *,
    benchmark_curve: pd.DataFrame | None = None,
    windows: list[StressWindow] | None = None,
) -> dict[str, Any]:
    cfg = config or CCELConfig()
    frames = {str(ticker).upper(): _normalize_frame(frame) for ticker, frame in market_frames.items() if not frame.empty}
    frames = {ticker: frame for ticker, frame in frames.items() if not frame.empty}
    if not frames:
        raise ValueError("CCEL backtest requires at least one frame.")
    dates = _panel_dates(frames)
    if len(dates) < 2:
        raise ValueError("CCEL backtest requires at least two trading dates.")

    cash = float(cfg.starting_cash)
    lots: list[CCELLot] = []
    trades: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    pending: PendingInstruction | None = PendingInstruction(decision_date=_date_text(dates[0]), buy_candidates=sorted(frames), reason="initial_deployment")
    next_lot_id = 1
    no_rebuy_until: dict[str, pd.Timestamp] = {}
    pending_reentry: dict[str, dict[str, Any]] = {}
    strategic_entry_dates: dict[str, str] = {}
    equity_curve: list[dict[str, Any]] = []
    total_costs = 0.0
    total_turnover = 0.0
    last_close: dict[str, float] = {}

    for idx, date in enumerate(dates):
        active = {ticker for ticker, frame in frames.items() if date in frame.index}
        if not active:
            continue
        open_prices = {ticker: float(frames[ticker].loc[date, "open"]) for ticker in active}
        if pending is not None:
            cash, next_lot_id, trade_rows, realized_rows, cost_delta, turnover_delta = _execute_ccel_instruction(
                date=date,
                pending=pending,
                lots=lots,
                cash=cash,
                open_prices=open_prices,
                no_rebuy_until=no_rebuy_until,
                pending_reentry=pending_reentry,
                cfg=cfg,
                next_lot_id=next_lot_id,
            )
            trades.extend(trade_rows)
            realizations.extend(realized_rows)
            _update_strategic_entry_dates(strategic_entry_dates, lots, trade_rows)
            total_costs += cost_delta
            total_turnover += turnover_delta
            pending = None

        close_prices = dict(last_close)
        for ticker in active:
            close_prices[ticker] = float(frames[ticker].loc[date, "price"])
        last_close.update(close_prices)
        position_value = _position_value(lots, close_prices)
        equity = cash + position_value
        exposure = position_value / equity if equity > 0 else 0.0
        equity_curve.append(
            {
                "date": _date_text(date),
                "equity": equity,
                "cash": cash,
                "position_value": position_value,
                "exposure": exposure,
                "costs_paid": 0.0,
                "turnover": 0.0,
                "open_lot_count": len(lots),
            }
        )
        if idx >= len(dates) - 1:
            continue
        pending = _build_ccel_instruction(
            date=date,
            frames=frames,
            active=active,
            lots=lots,
            cash=cash,
            close_prices=close_prices,
            no_rebuy_until=no_rebuy_until,
            pending_reentry=pending_reentry,
            cfg=cfg,
            first_day=(idx == 0),
            is_month_start=(idx == 0 or pd.Timestamp(dates[idx - 1]).month != pd.Timestamp(date).month),
            strategic_entry_dates=strategic_entry_dates,
        )

    taxable = apply_wash_sales(realizations, trades)
    after_tax_curve, tax_summary = build_after_tax_curve(
        equity_curve,
        taxable,
        lots,
        last_close,
        st_tax_rate=cfg.st_tax_rate,
        lt_tax_rate=cfg.lt_tax_rate,
    )
    after_tax_metrics = _metrics(after_tax_curve, trades, benchmark_curve=benchmark_curve)
    pre_tax_metrics = _metrics(equity_curve, trades, benchmark_curve=benchmark_curve)
    after_tax_metrics["annualized_turnover"] = _annualized_turnover(total_turnover, len(equity_curve))
    after_tax_metrics["total_turnover"] = total_turnover
    after_tax_metrics["total_costs_paid"] = total_costs
    after_tax_metrics["after_tax_terminal_wealth"] = after_tax_curve[-1]["equity"] if after_tax_curve else None
    after_tax_metrics["pre_tax_terminal_wealth"] = equity_curve[-1]["equity"] if equity_curve else None
    after_tax_metrics["terminal_tax_liability"] = tax_summary.get("terminal_tax_liability")
    after_tax_metrics["taxes_paid"] = tax_summary.get("taxes_paid")
    oos_start = pd.Timestamp(cfg.oos_start) if cfg.oos_start else None
    in_sample = _segment_metrics(after_tax_curve, trades, benchmark_curve, None, oos_start)
    out_of_sample = _segment_metrics(after_tax_curve, trades, benchmark_curve, oos_start, None) if oos_start is not None else None
    stress_benchmark = benchmark_curve if benchmark_curve is not None else pd.DataFrame()
    stress = _stress_results_for_curve(pd.DataFrame(after_tax_curve), trades, stress_benchmark, windows or [])
    return {
        "schema": "regime_ccel_backtest.v1",
        "proxy_label": PROXY_LABEL,
        "config": cfg.to_dict(),
        "strategy_spec": {"name": "CCEL_v1a_research_proxy", "research_only": True},
        "strategy_hash": _stable_hash(cfg.to_dict()),
        "git_sha": _git_sha(),
        "metrics": _json_safe(after_tax_metrics),
        "pre_tax_metrics": _json_safe(pre_tax_metrics),
        "in_sample": _json_safe(in_sample),
        "out_of_sample": _json_safe(out_of_sample),
        "equity_curve": _json_safe(equity_curve),
        "after_tax_equity_curve": _json_safe(after_tax_curve),
        "trades": _json_safe(trades),
        "realized_lots": _json_safe(taxable),
        "tax_summary": _json_safe(tax_summary),
        "stress_windows": _json_safe(stress),
        "open_lots": _json_safe([lot.to_dict() for lot in lots]),
        "pending_reentry": _json_safe(pending_reentry),
        "strategic_entry_dates": _json_safe(strategic_entry_dates),
    }


def apply_wash_sales(realizations: list[dict[str, Any]], trades: list[dict[str, Any]], *, window_days: int = 30) -> list[dict[str, Any]]:
    buys = [
        dict(row)
        for row in trades
        if str(row.get("side") or "").lower() == "buy" and float(row.get("quantity") or 0.0) > 0
    ]
    adjusted: list[dict[str, Any]] = []
    for row in realizations:
        item = dict(row)
        gain = float(item.get("gain") or 0.0)
        qty = float(item.get("quantity") or 0.0)
        disallowed = 0.0
        if gain < 0 and qty > 0:
            sale_date = pd.Timestamp(item.get("date"))
            ticker = str(item.get("ticker") or "").upper()
            replacement_qty = 0.0
            for buy in buys:
                if str(buy.get("ticker") or "").upper() != ticker:
                    continue
                buy_date = pd.Timestamp(buy.get("date"))
                if abs((buy_date - sale_date).days) <= int(window_days):
                    replacement_qty += float(buy.get("quantity") or 0.0)
            disallowed = min(1.0, replacement_qty / qty) * abs(gain) if replacement_qty > 0 else 0.0
        item["wash_disallowed_loss"] = disallowed
        item["tax_gain"] = gain + disallowed
        adjusted.append(item)
    return adjusted


def annual_tax(realizations: list[dict[str, Any]], *, st_tax_rate: float = 0.32, lt_tax_rate: float = 0.20) -> dict[str, Any]:
    by_year: dict[int, dict[str, float]] = {}
    for row in realizations:
        year = int(pd.Timestamp(row.get("date")).year)
        bucket = by_year.setdefault(year, {"st": 0.0, "lt": 0.0, "disallowed": 0.0})
        gain = float(row.get("tax_gain", row.get("gain", 0.0)) or 0.0)
        if str(row.get("term") or "ST") == "LT":
            bucket["lt"] += gain
        else:
            bucket["st"] += gain
        bucket["disallowed"] += float(row.get("wash_disallowed_loss") or 0.0)
    carryforward = 0.0
    rows: list[dict[str, Any]] = []
    taxes_paid = 0.0
    for year in sorted(by_year):
        st = float(by_year[year]["st"])
        lt = float(by_year[year]["lt"])
        if carryforward < 0:
            st, carryforward = _apply_carry_to_gain(st, carryforward)
            lt, carryforward = _apply_carry_to_gain(lt, carryforward)
        st, lt = _net_st_lt(st, lt)
        tax = max(0.0, st) * float(st_tax_rate) + max(0.0, lt) * float(lt_tax_rate)
        carryforward += min(0.0, st) + min(0.0, lt)
        taxes_paid += tax
        rows.append(
            {
                "year": year,
                "short_term_tax_gain": st,
                "long_term_tax_gain": lt,
                "tax_paid": tax,
                "loss_carryforward": carryforward,
                "wash_disallowed_loss": by_year[year]["disallowed"],
            }
        )
    return {"by_year": rows, "taxes_paid": taxes_paid, "loss_carryforward": carryforward}


def build_after_tax_curve(
    equity_curve: list[dict[str, Any]],
    realizations: list[dict[str, Any]],
    open_lots: list[CCELLot],
    final_prices: dict[str, float],
    *,
    st_tax_rate: float = 0.32,
    lt_tax_rate: float = 0.20,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    tax = annual_tax(realizations, st_tax_rate=st_tax_rate, lt_tax_rate=lt_tax_rate)
    taxes_by_year = {int(row["year"]): float(row.get("tax_paid") or 0.0) for row in tax.get("by_year") or []}
    paid = 0.0
    output: list[dict[str, Any]] = []
    last_year: int | None = None
    for row in equity_curve:
        year = pd.Timestamp(row["date"]).year
        if last_year is not None and year != last_year:
            paid += taxes_by_year.get(last_year, 0.0)
        copied = dict(row)
        copied["pre_tax_equity"] = copied.get("equity")
        copied["taxes_paid_to_date"] = paid
        copied["equity"] = max(0.0, float(copied.get("equity") or 0.0) - paid)
        output.append(copied)
        last_year = year
    if output and last_year is not None:
        paid += taxes_by_year.get(last_year, 0.0)
        terminal_tax = terminal_liquidation_tax(
            open_lots,
            final_prices,
            as_of=pd.Timestamp(output[-1]["date"]),
            carryforward=float(tax.get("loss_carryforward") or 0.0),
            st_tax_rate=st_tax_rate,
            lt_tax_rate=lt_tax_rate,
        )
        output[-1]["taxes_paid_to_date"] = paid
        output[-1]["terminal_tax_liability"] = terminal_tax
        output[-1]["equity"] = max(0.0, float(output[-1].get("pre_tax_equity") or 0.0) - paid - terminal_tax)
        tax["terminal_tax_liability"] = terminal_tax
        tax["taxes_paid"] = paid
        tax["after_tax_terminal_wealth"] = output[-1]["equity"]
    return output, tax


def terminal_liquidation_tax(
    open_lots: list[CCELLot],
    final_prices: dict[str, float],
    *,
    as_of: pd.Timestamp,
    carryforward: float,
    st_tax_rate: float,
    lt_tax_rate: float,
) -> float:
    synthetic: list[dict[str, Any]] = []
    for lot in open_lots:
        price = float(final_prices.get(lot.ticker, 0.0))
        if price <= 0 or lot.quantity <= 0:
            continue
        holding_days = (pd.Timestamp(as_of).normalize() - pd.Timestamp(lot.acquisition_date).normalize()).days
        synthetic.append(
            {
                "date": pd.Timestamp(as_of).date().isoformat(),
                "ticker": lot.ticker,
                "quantity": lot.quantity,
                "gain": (price - lot.basis_per_share) * lot.quantity,
                "tax_gain": (price - lot.basis_per_share) * lot.quantity,
                "term": "LT" if holding_days > 365 else "ST",
            }
        )
    if carryforward < 0:
        synthetic.append({"date": pd.Timestamp(as_of).date().isoformat(), "ticker": "__carryforward__", "quantity": 0.0, "gain": carryforward, "tax_gain": carryforward, "term": "ST"})
    return float(annual_tax(synthetic, st_tax_rate=st_tax_rate, lt_tax_rate=lt_tax_rate).get("taxes_paid") or 0.0)


def render_ccel_report(
    *,
    campaign_dir: str | Path = DEFAULT_CCEL_CAMPAIGN_DIR,
    output_dir: str | Path = DEFAULT_CCEL_REPORT_DIR,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(campaign_dir)
    summary = dict(_read_json(root / "summary.json"))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = out_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    results = _load_results(summary)
    charts = _build_ccel_charts(summary, results, assets_dir)
    report_path = Path(output_path) if output_path is not None else out_dir / "management_report.html"
    report_path.write_text(_ccel_report_html(summary, charts, results), encoding="utf-8")
    return report_path


def ccel_campaign_status(campaign_dir: str | Path = DEFAULT_CCEL_CAMPAIGN_DIR) -> dict[str, Any]:
    root = Path(campaign_dir)
    return {
        "campaign_dir": str(root),
        "summary_exists": (root / "summary.json").exists(),
        "cache_manifest_exists": (root / "cache_manifest.json").exists(),
        "result_count": len(list((root / "results").glob("*.json"))) if (root / "results").exists() else 0,
    }


def ccel_verdict(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_arm = {str(row.get("arm")): row for row in rows}
    ccel = by_arm.get("CCEL_v1a") or {}
    benchmarks = ["QQQ_buy_hold", "SPY_buy_hold", "L1"]
    terminal_ok = all((_float(ccel.get("after_tax_terminal_wealth")) or 0.0) > (_float((by_arm.get(arm) or {}).get("after_tax_terminal_wealth")) or math.inf) for arm in benchmarks)
    calmar_ok = all((_float(ccel.get("calmar_ratio")) or -math.inf) > (_float((by_arm.get(arm) or {}).get("calmar_ratio")) or math.inf) for arm in benchmarks)
    ulcer_ok = all((_float(ccel.get("ulcer_index")) or math.inf) < (_float((by_arm.get(arm) or {}).get("ulcer_index")) or -math.inf) for arm in benchmarks)
    qqq = by_arm.get("QQQ_buy_hold") or {}
    max_dd = abs(_float(ccel.get("max_drawdown")) or 0.0)
    qqq_dd = abs(_float(qqq.get("max_drawdown")) or 0.0)
    dd_ok = qqq_dd <= 0 or max_dd <= qqq_dd * 1.20
    risk_bar_ok = bool(calmar_ok and ulcer_ok and dd_ok)
    strict_proxy_bar = bool(terminal_ok and risk_bar_ok)
    not_killed = bool(terminal_ok and dd_ok)
    if strict_proxy_bar:
        status = "proxy_bar_cleared_not_validated"
        next_step = "Proceed to v1b rolling universe as research only; no production changes until survivorship-free evidence exists."
    elif not_killed:
        status = "not_killed_not_validated"
        next_step = "Proceed to v1b rolling universe only as an exploratory research gate; risk/Ulcer evidence is not strong enough for production."
    else:
        status = "killed"
        next_step = "Stop CCEL v1b and write up the kill-switch result."
    return {
        "ccel_v1a_clears_proxy_bar": strict_proxy_bar,
        "ccel_v1a_not_killed": not_killed,
        "v1b_research_gate_open": not_killed,
        "validation_status": status,
        "recommended_next_step": next_step,
        "terminal_wealth_ok": terminal_ok,
        "calmar_ok": calmar_ok,
        "ulcer_ok": ulcer_ok,
        "risk_bar_ok": risk_bar_ok,
        "drawdown_within_tolerance": dd_ok,
        "production_default_changes": [],
        "proxy_caveat": "A pass does not certify edge because the current basket is survivorship-biased; a fail is a kill-switch.",
    }


def ccel_limitations(*, start: str, end: str, load_errors: dict[str, str]) -> list[str]:
    limitations = _limitations(start=start, end=end, load_errors=load_errors)
    limitations.extend(
        [
            PROXY_LABEL,
            "The v1a basket uses current campaign constituents, so survivorship and hindsight bias can inflate results.",
            "Point-in-time fundamentals and delisted securities are not available in this proxy; fundamental deterioration exits only fire when historical quality columns are supplied.",
            "FIFO lot selection is used as a conservative floor; HIFO/specific-lot optimization is intentionally deferred.",
            "Wash-sale handling disallows same-ticker losses when replacement buys occur within +/-30 calendar days; replacement basis adjustment is approximated conservatively as a loss deferral.",
            "Harvest replacement defaults to exposure-neutral redeployment into remaining holdings; momentum-ranked replacement is available only as an explicit research mode.",
        ]
    )
    return limitations


def _data_layer_status(
    *,
    data_source: str,
    loader: FrameLoader,
    tickers: list[str],
    start: str,
    end: str,
) -> tuple[dict[str, Any], str | None]:
    if data_source != "sharadar":
        readiness = {
            "data_readiness": "price_only_proxy",
            "price_coverage": True,
            "pit_fundamental_coverage": False,
            "universe_count": len(tickers),
            "resolved_count": 0,
            "missing_price": [],
            "missing_pit": sorted(set(tickers)),
            "data_snapshot_hash": None,
            "reasons": ["proxy_price_source"],
        }
        return readiness, None
    store = getattr(loader, "store", None)
    if store is None:
        readiness = {
            "data_readiness": "partial_pit",
            "price_coverage": True,
            "pit_fundamental_coverage": False,
            "universe_count": len(tickers),
            "resolved_count": 0,
            "missing_price": [],
            "missing_pit": [],
            "data_snapshot_hash": None,
            "reasons": ["custom_sharadar_loader_without_store_metadata"],
        }
        return readiness, None
    result = classify_readiness(store, tickers, (start, end)).to_dict()
    snapshot_hash = result.get("data_snapshot_hash")
    return dict(result), str(snapshot_hash) if snapshot_hash else None


def _stamp_data_layer(
    payload: dict[str, Any],
    *,
    data_source: str,
    readiness: dict[str, Any],
    snapshot_hash: str | None,
) -> dict[str, Any]:
    copied = dict(payload)
    copied["data_source"] = data_source
    copied["data_readiness"] = readiness.get("data_readiness")
    copied["readiness"] = readiness
    copied["data_snapshot_hash"] = snapshot_hash
    copied["production_defaults_changed"] = bool(copied.get("production_defaults_changed", False))
    return copied


def _adjusted_price_note(data_source: str) -> str:
    if data_source == "sharadar":
        return "Sharadar SEP adjusted daily OHLC is used with point-in-time SF1 fundamentals where available."
    return "Adjusted daily OHLC is used where available; this is a survivorship-biased proxy using the current campaign basket."


def _data_source_cli_args(data_source: str, sharadar_store_dir: str | Path) -> str:
    if data_source != "sharadar":
        return ""
    return f"--data-source sharadar --sharadar-store-dir {Path(sharadar_store_dir)} "


def _data_layer_limitations(data_source: str, readiness: dict[str, Any]) -> list[str]:
    if data_source != "sharadar":
        return []
    status = str(readiness.get("data_readiness") or "")
    if status == "survivorship_free":
        return ["Sharadar run uses a frozen local snapshot; certification still requires after-tax OOS evidence and promotion-gate review."]
    return [
        "Sharadar data source selected, but this run is not certifiable because local snapshot readiness is not survivorship_free.",
        f"Sharadar readiness reasons: {', '.join(str(item) for item in readiness.get('reasons') or [])}",
    ]


def _run_reference_arms(
    *,
    raw_frames: dict[str, pd.DataFrame],
    benchmarks: dict[str, pd.DataFrame],
    start: str,
    end: str,
    oos_start: str,
    windows: list[StressWindow],
    benchmark_curve: pd.DataFrame,
    regime_enricher: Callable[[dict[str, pd.DataFrame]], dict[str, pd.DataFrame]] | None,
) -> dict[str, dict[str, Any]]:
    cfg = PortfolioBacktestConfig(oos_start=oos_start, availability_mode="panel")
    enriched = regime_enricher(raw_frames) if regime_enricher is not None else raw_frames
    timing = with_market_timing_signal(enriched, benchmarks["SPY"])
    specs = campaign2_headline_specs()
    out: dict[str, dict[str, Any]] = {}
    l1_result = run_portfolio_backtest(enriched, specs["L1"], cfg, benchmark_curve=benchmark_curve, windows=windows).to_dict()
    out["L1"] = tax_adjust_portfolio_payload(l1_result, raw_frames, oos_start=oos_start, benchmark_curve=benchmark_curve)
    winner_arm, winner_spec = _campaign3_winner_spec(start, end)
    arm_frames = timing if winner_spec.override_policy == "market_timing_brake" else enriched
    winner_result = run_portfolio_backtest(arm_frames, winner_spec, cfg, benchmark_curve=benchmark_curve, windows=windows).to_dict()
    winner_payload = tax_adjust_portfolio_payload(winner_result, raw_frames, oos_start=oos_start, benchmark_curve=benchmark_curve)
    winner_payload["campaign3_winner_arm"] = winner_arm
    out["Campaign3_winner"] = winner_payload
    out["SPY_buy_hold"] = buy_hold_taxable_payload("SPY", benchmarks["SPY"], oos_start=oos_start, benchmark_curve=benchmark_curve, windows=windows)
    out["QQQ_buy_hold"] = buy_hold_taxable_payload("QQQ", benchmarks["QQQ"], oos_start=oos_start, benchmark_curve=benchmark_curve, windows=windows)
    return out


def tax_adjust_portfolio_payload(
    payload: dict[str, Any],
    frames: dict[str, pd.DataFrame],
    *,
    oos_start: str,
    benchmark_curve: pd.DataFrame | None = None,
    st_tax_rate: float = 0.32,
    lt_tax_rate: float = 0.20,
) -> dict[str, Any]:
    trades = [dict(row) for row in payload.get("trades") or [] if isinstance(row, dict)]
    lots, realizations = reconstruct_lots_from_trades(trades)
    final_prices = {
        ticker: float(_normalize_frame(frame)["price"].iloc[-1])
        for ticker, frame in frames.items()
        if not _normalize_frame(frame).empty
    }
    taxable = apply_wash_sales(realizations, trades)
    source_curve = [dict(row) for row in payload.get("equity_curve") or [] if isinstance(row, dict)]
    after_tax_curve, tax_summary = build_after_tax_curve(source_curve, taxable, lots, final_prices, st_tax_rate=st_tax_rate, lt_tax_rate=lt_tax_rate)
    adjusted = dict(payload)
    adjusted["pre_tax_metrics"] = dict(payload.get("metrics") or {})
    metrics = _metrics(after_tax_curve, trades, benchmark_curve=benchmark_curve)
    metrics["annualized_turnover"] = (payload.get("metrics") or {}).get("annualized_turnover")
    metrics["total_turnover"] = (payload.get("metrics") or {}).get("total_turnover")
    metrics["total_costs_paid"] = (payload.get("metrics") or {}).get("total_costs_paid")
    metrics["after_tax_terminal_wealth"] = after_tax_curve[-1]["equity"] if after_tax_curve else None
    metrics["pre_tax_terminal_wealth"] = source_curve[-1]["equity"] if source_curve else None
    metrics["terminal_tax_liability"] = tax_summary.get("terminal_tax_liability")
    metrics["taxes_paid"] = tax_summary.get("taxes_paid")
    adjusted["metrics"] = _json_safe(metrics)
    adjusted["after_tax_equity_curve"] = _json_safe(after_tax_curve)
    adjusted["realized_lots"] = _json_safe(taxable)
    adjusted["tax_summary"] = _json_safe(tax_summary)
    oos = pd.Timestamp(oos_start) if oos_start else None
    adjusted["in_sample"] = _json_safe(_segment_metrics(after_tax_curve, trades, benchmark_curve, None, oos))
    adjusted["out_of_sample"] = _json_safe(_segment_metrics(after_tax_curve, trades, benchmark_curve, oos, None) if oos is not None else None)
    return adjusted


def buy_hold_taxable_payload(
    ticker: str,
    frame: pd.DataFrame,
    *,
    oos_start: str,
    benchmark_curve: pd.DataFrame,
    windows: list[StressWindow],
    starting_cash: float = 100_000.0,
    entry_cost_bps: float = 5.0,
    st_tax_rate: float = 0.32,
    lt_tax_rate: float = 0.20,
) -> dict[str, Any]:
    normalized = _normalize_frame(frame)
    first = normalized.iloc[0]
    price = float(first["open"])
    unit_cost = price * (1.0 + entry_cost_bps / 10_000.0)
    qty = math.floor(starting_cash / unit_cost)
    cost = qty * price * entry_cost_bps / 10_000.0
    cash = starting_cash - qty * price - cost
    lot = CCELLot(1, str(ticker).upper(), qty, unit_cost, _date_text(normalized.index[0]))
    trades = [
        {
            "date": _date_text(normalized.index[0]),
            "ticker": str(ticker).upper(),
            "side": "Buy",
            "quantity": qty,
            "price": price,
            "notional": qty * price,
            "costs_paid": cost,
            "net_pnl": -cost,
            "exit_type": "initial_buy_hold",
        }
    ]
    equity_curve = [
        {
            "date": _date_text(index),
            "equity": cash + qty * float(row["price"]),
            "cash": cash,
            "position_value": qty * float(row["price"]),
            "exposure": (qty * float(row["price"])) / (cash + qty * float(row["price"])) if cash + qty * float(row["price"]) > 0 else 0.0,
            "costs_paid": 0.0,
            "turnover": 0.0,
        }
        for index, row in normalized.iterrows()
    ]
    after_tax_curve, tax_summary = build_after_tax_curve(
        equity_curve,
        [],
        [lot],
        {str(ticker).upper(): float(normalized["price"].iloc[-1])},
        st_tax_rate=st_tax_rate,
        lt_tax_rate=lt_tax_rate,
    )
    metrics = _metrics(after_tax_curve, trades, benchmark_curve=benchmark_curve)
    total_turnover = qty * price / starting_cash if starting_cash else 0.0
    metrics["annualized_turnover"] = _annualized_turnover(total_turnover, len(equity_curve))
    metrics["total_turnover"] = total_turnover
    metrics["total_costs_paid"] = cost
    metrics["after_tax_terminal_wealth"] = after_tax_curve[-1]["equity"] if after_tax_curve else None
    metrics["pre_tax_terminal_wealth"] = equity_curve[-1]["equity"] if equity_curve else None
    metrics["terminal_tax_liability"] = tax_summary.get("terminal_tax_liability")
    metrics["taxes_paid"] = tax_summary.get("taxes_paid")
    oos = pd.Timestamp(oos_start) if oos_start else None
    return {
        "schema": "regime_ccel_buy_hold_benchmark.v1",
        "proxy_label": PROXY_LABEL,
        "config": {"ticker": str(ticker).upper(), "starting_cash": starting_cash, "entry_cost_bps": entry_cost_bps, "oos_start": oos_start},
        "strategy_spec": {"name": f"{ticker}_buy_hold_after_tax"},
        "metrics": _json_safe(metrics),
        "pre_tax_metrics": _json_safe(_metrics(equity_curve, trades, benchmark_curve=benchmark_curve)),
        "in_sample": _json_safe(_segment_metrics(after_tax_curve, trades, benchmark_curve, None, oos)),
        "out_of_sample": _json_safe(_segment_metrics(after_tax_curve, trades, benchmark_curve, oos, None) if oos is not None else None),
        "equity_curve": _json_safe(equity_curve),
        "after_tax_equity_curve": _json_safe(after_tax_curve),
        "trades": _json_safe(trades),
        "realized_lots": [],
        "tax_summary": _json_safe(tax_summary),
        "stress_windows": _json_safe(_stress_results_for_curve(pd.DataFrame(after_tax_curve), trades, benchmark_curve, windows)),
    }


def reconstruct_lots_from_trades(trades: list[dict[str, Any]]) -> tuple[list[CCELLot], list[dict[str, Any]]]:
    lots: list[CCELLot] = []
    realizations: list[dict[str, Any]] = []
    next_id = 1
    for trade in sorted(trades, key=lambda row: (str(row.get("date") or ""), 0 if str(row.get("side")).lower() == "buy" else 1)):
        ticker = str(trade.get("ticker") or "").upper()
        qty = float(trade.get("quantity") or 0.0)
        price = float(trade.get("price") or 0.0)
        cost = float(trade.get("costs_paid") or 0.0)
        date = str(trade.get("date") or "")
        if not ticker or qty <= 0 or price <= 0 or not date:
            continue
        if str(trade.get("side") or "").lower() == "buy":
            basis = price + (cost / qty if qty else 0.0)
            lots.append(CCELLot(next_id, ticker, qty, basis, date))
            next_id += 1
        elif str(trade.get("side") or "").lower() == "sell":
            proceeds_per_share = price - (cost / qty if qty else 0.0)
            realizations.extend(_sell_fifo_lots(lots, ticker, qty, proceeds_per_share, date, str(trade.get("exit_type") or "sell")))
    return lots, realizations


def _execute_ccel_instruction(
    *,
    date: pd.Timestamp,
    pending: PendingInstruction,
    lots: list[CCELLot],
    cash: float,
    open_prices: dict[str, float],
    no_rebuy_until: dict[str, pd.Timestamp],
    pending_reentry: dict[str, dict[str, Any]] | None,
    cfg: CCELConfig,
    next_lot_id: int,
) -> tuple[float, int, list[dict[str, Any]], list[dict[str, Any]], float, float]:
    trades: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    pending_reentry = pending_reentry if pending_reentry is not None else {}
    if pending.harvest_reentry_targets:
        cash, next_lot_id, reentry_trades, reentry_realized, reentry_costs, reentry_turnover = _execute_harvest_reentries(
            date=date,
            targets=pending.harvest_reentry_targets,
            lots=lots,
            cash=cash,
            open_prices=open_prices,
            pending_reentry=pending_reentry,
            cfg=cfg,
            next_lot_id=next_lot_id,
        )
        trades.extend(reentry_trades)
        realizations.extend(reentry_realized)
        costs += reentry_costs
        turnover += reentry_turnover
    harvest_replacement_cash = 0.0
    for ticker, reason in sorted(pending.sell_tickers.items()):
        qty = sum(lot.quantity for lot in lots if lot.ticker == ticker)
        price = float(open_prices.get(ticker, 0.0))
        if qty <= 0 or price <= 0:
            continue
        total_before_sale = cash + _position_value(lots, open_prices)
        target_weight = qty * price / total_before_sale if total_before_sale > 0 else 0.0
        bridge_target_weights = {
            held: sum(lot.quantity for lot in lots if lot.ticker == held) * float(open_prices.get(held, 0.0)) / total_before_sale
            for held in sorted(_held_tickers(lots))
            if held != ticker and total_before_sale > 0 and float(open_prices.get(held, 0.0)) > 0
        }
        cost = qty * price * cfg.exit_cost_bps / 10_000.0
        proceeds_per_share = price - (cost / qty if qty else 0.0)
        realized = _sell_fifo_lots(lots, ticker, qty, proceeds_per_share, _date_text(date), reason)
        gain = sum(float(row.get("gain") or 0.0) for row in realized)
        if gain < 0:
            no_rebuy_until[ticker] = pd.Timestamp(date) + pd.Timedelta(days=cfg.no_rebuy_days)
        sale_proceeds = qty * price - cost
        if reason == "loss_harvest" and cfg.harvest_replacement_mode == "exposure_neutral":
            harvest_replacement_cash += sale_proceeds
            if cfg.harvest_reentry:
                pending_reentry[ticker] = {
                    "ticker": ticker,
                    "target_weight": target_weight,
                    "earliest_rebuy_date": _date_text(pd.Timestamp(date) + pd.Timedelta(days=cfg.no_rebuy_days)),
                    "bridge_target_weights": bridge_target_weights,
                    "harvest_date": _date_text(date),
                }
        cash += sale_proceeds
        costs += cost
        turnover += qty * price
        trades.append(_trade_row(date, ticker, "Sell", qty, price, qty * price, cost, gain, reason))
        realizations.extend(realized)
    if (
        cfg.harvest_replacement_mode == "exposure_neutral"
        and harvest_replacement_cash >= cfg.min_cash_to_deploy
        and pending.harvest_replacement_tickers
    ):
        weights = _held_value_weights(pending.harvest_replacement_tickers, lots, open_prices)
        cash, next_lot_id, replacement_trades, replacement_costs, replacement_turnover = _deploy_cash_to_tickers(
            date=date,
            tickers=pending.harvest_replacement_tickers,
            lots=lots,
            cash=cash,
            open_prices=open_prices,
            cfg=cfg,
            next_lot_id=next_lot_id,
            budget=min(harvest_replacement_cash, cash),
            reason="harvest_exposure_neutral",
            weights=weights,
        )
        trades.extend(replacement_trades)
        costs += replacement_costs
        turnover += replacement_turnover
    candidates = [
        ticker
        for ticker in pending.buy_candidates
        if ticker in open_prices and pd.Timestamp(date) > no_rebuy_until.get(ticker, pd.Timestamp.min)
    ]
    if cash >= cfg.min_cash_to_deploy and candidates:
        cash, next_lot_id, buy_trades, buy_costs, buy_turnover = _deploy_cash_to_tickers(
            date=date,
            tickers=candidates,
            lots=lots,
            cash=cash,
            open_prices=open_prices,
            cfg=cfg,
            next_lot_id=next_lot_id,
            budget=cash,
            reason=pending.reason or "buy_deploy",
        )
        trades.extend(buy_trades)
        costs += buy_costs
        turnover += buy_turnover
    return cash, next_lot_id, trades, realizations, costs, turnover / max(1.0, cfg.starting_cash)


def _update_strategic_entry_dates(
    strategic_entry_dates: dict[str, str],
    lots: list[CCELLot],
    trades: list[dict[str, Any]],
) -> None:
    held = _held_tickers(lots)
    for trade in trades:
        ticker = str(trade.get("ticker") or "").upper()
        if not ticker:
            continue
        side = str(trade.get("side") or "").lower()
        reason = str(trade.get("exit_type") or "")
        date = str(trade.get("date") or "")
        if side == "buy":
            if reason != "harvest_reentry" or ticker not in strategic_entry_dates:
                strategic_entry_dates.setdefault(ticker, date)
        elif side == "sell" and ticker not in held and reason != "loss_harvest":
            strategic_entry_dates.pop(ticker, None)


def _execute_harvest_reentries(
    *,
    date: pd.Timestamp,
    targets: dict[str, float],
    lots: list[CCELLot],
    cash: float,
    open_prices: dict[str, float],
    pending_reentry: dict[str, dict[str, Any]],
    cfg: CCELConfig,
    next_lot_id: int,
) -> tuple[float, int, list[dict[str, Any]], list[dict[str, Any]], float, float]:
    trades: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    for ticker, target_weight in sorted(targets.items()):
        if ticker not in open_prices or float(open_prices.get(ticker, 0.0)) <= 0:
            continue
        total_value = cash + _position_value(lots, open_prices)
        if total_value <= 0:
            continue
        current_value = sum(lot.quantity for lot in lots if lot.ticker == ticker) * float(open_prices[ticker])
        target_value = max(0.0, float(target_weight or 0.0)) * total_value
        buy_budget = max(0.0, target_value - current_value)
        if buy_budget < cfg.min_cash_to_deploy:
            pending_reentry.pop(ticker, None)
            continue
        entry_multiplier = 1.0 + cfg.entry_cost_bps / 10_000.0
        required_cash = buy_budget * entry_multiplier
        if cash < required_cash:
            needed = required_cash - cash
            cash, funding_trades, funding_realized, funding_costs, funding_turnover = _trim_bridge_for_reentry(
                date=date,
                ticker=ticker,
                needed_cash=needed,
                lots=lots,
                cash=cash,
                open_prices=open_prices,
                entry=pending_reentry.get(ticker) or {},
                cfg=cfg,
            )
            trades.extend(funding_trades)
            realizations.extend(funding_realized)
            costs += funding_costs
            turnover += funding_turnover
        before_lot_count = len(lots)
        cash, next_lot_id, buy_trades, buy_costs, buy_turnover = _deploy_cash_to_tickers(
            date=date,
            tickers=[ticker],
            lots=lots,
            cash=cash,
            open_prices=open_prices,
            cfg=cfg,
            next_lot_id=next_lot_id,
            budget=min(buy_budget, cash),
            reason="harvest_reentry",
        )
        trades.extend(buy_trades)
        costs += buy_costs
        turnover += buy_turnover
        bought = len(lots) > before_lot_count or any(row.get("ticker") == ticker and row.get("side") == "Buy" for row in buy_trades)
        if bought:
            pending_reentry.pop(ticker, None)
    return cash, next_lot_id, trades, realizations, costs, turnover


def _trim_bridge_for_reentry(
    *,
    date: pd.Timestamp,
    ticker: str,
    needed_cash: float,
    lots: list[CCELLot],
    cash: float,
    open_prices: dict[str, float],
    entry: dict[str, Any],
    cfg: CCELConfig,
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]], float, float]:
    bridge_weights = {
        str(name).upper(): max(0.0, float(weight or 0.0))
        for name, weight in dict(entry.get("bridge_target_weights") or {}).items()
    }
    bridge_tickers = [name for name in bridge_weights if name != ticker and name in open_prices]
    if not bridge_tickers:
        bridge_tickers = [name for name in sorted(_held_tickers(lots)) if name != ticker and name in open_prices]
    total_value = cash + _position_value(lots, open_prices)
    excess_values: dict[str, float] = {}
    for name in bridge_tickers:
        price = float(open_prices.get(name, 0.0))
        qty = sum(lot.quantity for lot in lots if lot.ticker == name)
        if price <= 0 or qty <= 0:
            continue
        current_value = qty * price
        target_value = bridge_weights.get(name, 0.0) * total_value if bridge_weights else 0.0
        excess = current_value - target_value
        if excess > 0:
            excess_values[name] = excess
    if not excess_values:
        return cash, [], [], 0.0, 0.0
    sell_notional = min(sum(excess_values.values()), needed_cash / max(1e-9, 1.0 - cfg.exit_cost_bps / 10_000.0))
    trades: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    excess_total = sum(excess_values.values())
    for name, excess in sorted(excess_values.items()):
        price = float(open_prices.get(name, 0.0))
        held_qty = sum(lot.quantity for lot in lots if lot.ticker == name)
        if price <= 0 or held_qty <= 0:
            continue
        allocation = sell_notional * excess / excess_total if excess_total > 0 else 0.0
        qty = math.floor(allocation / price) if cfg.integer_shares else allocation / price
        qty = min(qty, held_qty)
        if qty <= 0:
            continue
        cost = qty * price * cfg.exit_cost_bps / 10_000.0
        proceeds_per_share = price - (cost / qty if qty else 0.0)
        realized = _sell_fifo_lots(lots, name, qty, proceeds_per_share, _date_text(date), "harvest_reentry_funding")
        gain = sum(float(row.get("gain") or 0.0) for row in realized)
        cash += qty * price - cost
        costs += cost
        turnover += qty * price
        trades.append(_trade_row(date, name, "Sell", qty, price, qty * price, cost, gain, "harvest_reentry_funding"))
        realizations.extend(realized)
        if cash >= needed_cash:
            break
    return cash, trades, realizations, costs, turnover


def _deploy_cash_to_tickers(
    *,
    date: pd.Timestamp,
    tickers: list[str],
    lots: list[CCELLot],
    cash: float,
    open_prices: dict[str, float],
    cfg: CCELConfig,
    next_lot_id: int,
    budget: float,
    reason: str,
    weights: dict[str, float] | None = None,
) -> tuple[float, int, list[dict[str, Any]], float, float]:
    candidates = _unique_tickers([ticker for ticker in tickers if ticker in open_prices and float(open_prices.get(ticker, 0.0)) > 0])
    budget = min(float(budget), float(cash))
    if budget < cfg.min_cash_to_deploy or not candidates:
        return cash, next_lot_id, [], 0.0, 0.0
    if weights:
        weight_sum = sum(max(0.0, float(weights.get(ticker) or 0.0)) for ticker in candidates)
        allocations = {
            ticker: budget * max(0.0, float(weights.get(ticker) or 0.0)) / weight_sum
            for ticker in candidates
        } if weight_sum > 0 else {}
    else:
        allocations = {}
    if not allocations:
        per_name = budget / len(candidates)
        allocations = {ticker: per_name for ticker in candidates}
    trades: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    for ticker in candidates:
        price = float(open_prices.get(ticker, 0.0))
        if price <= 0:
            continue
        unit_cost = price * (1.0 + cfg.entry_cost_bps / 10_000.0)
        allocation = float(allocations.get(ticker) or 0.0)
        qty = math.floor(allocation / unit_cost) if cfg.integer_shares else allocation / unit_cost
        affordable = math.floor(cash / unit_cost) if cfg.integer_shares else cash / unit_cost
        qty = min(qty, affordable)
        if qty <= 0:
            continue
        cost = qty * price * cfg.entry_cost_bps / 10_000.0
        cash -= qty * price + cost
        costs += cost
        turnover += qty * price
        lots.append(CCELLot(next_lot_id, ticker, qty, unit_cost, _date_text(date)))
        next_lot_id += 1
        trades.append(_trade_row(date, ticker, "Buy", qty, price, qty * price, cost, -cost, reason))
    return cash, next_lot_id, trades, costs, turnover


def _build_ccel_instruction(
    *,
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    active: set[str],
    lots: list[CCELLot],
    cash: float,
    close_prices: dict[str, float],
    no_rebuy_until: dict[str, pd.Timestamp],
    pending_reentry: dict[str, dict[str, Any]] | None,
    cfg: CCELConfig,
    first_day: bool,
    is_month_start: bool,
    strategic_entry_dates: dict[str, str] | None = None,
) -> PendingInstruction | None:
    is_semiannual = pd.Timestamp(date).month in {1, 7} and is_month_start
    strategic_entry_dates = strategic_entry_dates if strategic_entry_dates is not None else {}
    harvest_reentry_targets: dict[str, float] = {}
    pending_reentry = pending_reentry if pending_reentry is not None else {}
    if cfg.harvest_reentry and cfg.harvest_replacement_mode == "exposure_neutral":
        for ticker, entry in list(pending_reentry.items()):
            earliest = pd.Timestamp(entry.get("earliest_rebuy_date", pd.Timestamp.max))
            if pd.Timestamp(date) < earliest:
                continue
            if ticker not in active or ticker not in frames or _quality_fails(ticker, date, frames):
                pending_reentry.pop(ticker, None)
                if ticker not in _held_tickers(lots):
                    strategic_entry_dates.pop(ticker, None)
                continue
            harvest_reentry_targets[ticker] = max(0.0, float(entry.get("target_weight") or 0.0))
    sell: dict[str, str] = {}
    if is_month_start:
        for ticker in sorted(_held_tickers(lots)):
            price = float(close_prices.get(ticker, 0.0))
            if price <= 0:
                continue
            position_lots = [lot for lot in lots if lot.ticker == ticker]
            if not position_lots:
                continue
            basis = _weighted_basis(position_lots)
            tax_lot_age = min((pd.Timestamp(date) - pd.Timestamp(lot.acquisition_date)).days for lot in position_lots)
            strategic_date = strategic_entry_dates.get(ticker)
            age = (pd.Timestamp(date) - pd.Timestamp(strategic_date)).days if strategic_date else max((pd.Timestamp(date) - pd.Timestamp(lot.acquisition_date)).days for lot in position_lots)
            pnl_pct = price / basis - 1.0 if basis > 0 else 0.0
            significant_short_gain = age <= cfg.probation_days and pnl_pct >= cfg.significant_gain_pct
            if significant_short_gain:
                continue
            if cfg.harvest_enabled and tax_lot_age >= cfg.min_harvest_age_days and pnl_pct <= -cfg.harvest_loss_pct:
                sell[ticker] = "loss_harvest"
                continue
            if not cfg.core_only and cfg.probation_enabled and age <= cfg.probation_days:
                if pnl_pct <= -cfg.probation_loss_pct:
                    sell[ticker] = "probation_relegate_loss"
                    continue
                if cfg.momentum_breakdown_enabled and _bottom_momentum(ticker, date, frames, active, cfg.momentum_bottom_quantile):
                    sell[ticker] = "probation_momentum_breakdown"
                    continue
            if _quality_fails(ticker, date, frames):
                sell[ticker] = "quality_gate_deterioration"
    need_deploy = first_day or is_semiannual or bool(sell) or bool(harvest_reentry_targets) or cash >= cfg.min_cash_to_deploy * 2
    if not need_deploy and not sell:
        return None
    held_after_sells = _held_tickers(lots) - set(sell)
    sell_reasons = set(sell.values())
    harvest_replacement_tickers: list[str] = []
    if cfg.harvest_replacement_mode == "exposure_neutral" and "loss_harvest" in sell_reasons:
        harvest_replacement_tickers = sorted(ticker for ticker in held_after_sells if ticker in active)
    has_selection_sale = any(reason != "loss_harvest" for reason in sell_reasons)
    harvest_uses_momentum = cfg.harvest_replacement_mode == "momentum" and "loss_harvest" in sell_reasons
    selection_trigger = first_day or is_semiannual or has_selection_sale or harvest_uses_momentum or cash >= cfg.min_cash_to_deploy * 2
    candidates: list[str] = []
    if selection_trigger:
        max_new = max(0, cfg.max_names - len(held_after_sells))
        candidates = _ranked_buy_candidates(date, frames, active, held_after_sells, set(sell), no_rebuy_until, max_new=max_new)
    if not sell and cash < cfg.min_cash_to_deploy and not candidates:
        return None
    return PendingInstruction(
        decision_date=_date_text(date),
        sell_tickers=sell,
        buy_candidates=candidates,
        harvest_replacement_tickers=harvest_replacement_tickers,
        harvest_reentry_targets=harvest_reentry_targets,
        reason="ccel_review",
    )


def _ranked_buy_candidates(
    date: pd.Timestamp,
    frames: dict[str, pd.DataFrame],
    active: set[str],
    held: set[str],
    just_sold: set[str],
    no_rebuy_until: dict[str, pd.Timestamp],
    *,
    max_new: int,
) -> list[str]:
    candidates = []
    for ticker in sorted(active):
        if ticker in just_sold:
            continue
        if pd.Timestamp(date) <= no_rebuy_until.get(ticker, pd.Timestamp.min):
            continue
        momentum = _momentum_12_1(ticker, date, frames)
        is_new = ticker not in held
        candidates.append((not is_new, -(momentum if momentum is not None else -999.0), ticker))
    candidates.sort()
    selected: list[str] = []
    for not_new, _score, ticker in candidates:
        if not_new and selected:
            continue
        if not not_new and max_new <= 0:
            continue
        selected.append(ticker)
        if not not_new:
            max_new -= 1
        if len(selected) >= 10:
            break
    return selected


def _sell_fifo_lots(lots: list[CCELLot], ticker: str, quantity: float, proceeds_per_share: float, date: str, reason: str) -> list[dict[str, Any]]:
    remaining = float(quantity)
    realized: list[dict[str, Any]] = []
    for lot in sorted([lot for lot in lots if lot.ticker == ticker and lot.quantity > 0], key=lambda item: (item.acquisition_date, item.lot_id)):
        if remaining <= 1e-9:
            break
        take = min(lot.quantity, remaining)
        holding_days = (pd.Timestamp(date) - pd.Timestamp(lot.acquisition_date)).days
        gain = (float(proceeds_per_share) - lot.basis_per_share) * take
        realized.append(
            {
                "date": date,
                "ticker": ticker,
                "quantity": take,
                "basis_per_share": lot.basis_per_share,
                "proceeds_per_share": proceeds_per_share,
                "gain": gain,
                "term": "LT" if holding_days > 365 else "ST",
                "holding_days": holding_days,
                "acquisition_date": lot.acquisition_date,
                "exit_reason": reason,
            }
        )
        lot.quantity -= take
        remaining -= take
    lots[:] = [lot for lot in lots if lot.quantity > 1e-9]
    return realized


def _trade_row(date: pd.Timestamp, ticker: str, side: str, quantity: float, price: float, notional: float, cost: float, net_pnl: float, reason: str) -> dict[str, Any]:
    return {
        "date": _date_text(date),
        "ticker": ticker,
        "side": side,
        "quantity": quantity,
        "price": price,
        "notional": notional,
        "costs_paid": cost,
        "net_pnl": net_pnl,
        "exit_date": _date_text(date),
        "exit_type": reason,
    }


def _metrics(equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]], *, benchmark_curve: pd.DataFrame | None = None) -> dict[str, Any]:
    frame = pd.DataFrame(equity_curve)
    metrics = compute_equity_metrics(frame, trades, benchmark_curve=benchmark_curve)
    metrics["calmar_ratio"] = _calmar(metrics)
    metrics["ulcer_index"] = ulcer_index(equity_curve)
    return metrics


def ulcer_index(equity_curve: list[dict[str, Any]]) -> float | None:
    if not equity_curve:
        return None
    equity = pd.to_numeric(pd.DataFrame(equity_curve)["equity"], errors="coerce").dropna()
    if equity.empty:
        return None
    dd = equity / equity.cummax() - 1.0
    return float(math.sqrt(float((dd * dd).mean())))


def _segment_metrics(
    equity_curve: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    benchmark_curve: pd.DataFrame | None,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> dict[str, Any]:
    frame = pd.DataFrame(equity_curve)
    if frame.empty:
        return {}
    dates = pd.to_datetime(frame["date"])
    mask = pd.Series(True, index=frame.index)
    if start is not None:
        mask &= dates >= start
    if end is not None:
        mask &= dates < end
    segment = frame.loc[mask].copy()
    segment_trades = [
        row for row in trades
        if (start is None or pd.Timestamp(row["date"]) >= start)
        and (end is None or pd.Timestamp(row["date"]) < end)
    ]
    bench = None
    if benchmark_curve is not None and not benchmark_curve.empty:
        bench_dates = pd.to_datetime(benchmark_curve["date"])
        bench_mask = pd.Series(True, index=benchmark_curve.index)
        if start is not None:
            bench_mask &= bench_dates >= start
        if end is not None:
            bench_mask &= bench_dates < end
        bench = benchmark_curve.loc[bench_mask].copy()
    return _metrics(segment.to_dict("records"), segment_trades, benchmark_curve=bench)


def _build_ccel_charts(summary: dict[str, Any], results: dict[str, dict[str, Any]], assets_dir: Path) -> dict[str, str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    charts: dict[str, str] = {}

    def save(fig, name: str) -> None:
        path = assets_dir / f"{name}.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160, bbox_inches="tight")
        plt.close(fig)
        charts[name] = str(path)

    ordered = [row for row in rows if str(row.get("arm")) in _report_arm_order()]
    labels = [_arm_label(str(row.get("arm"))) for row in ordered]
    if ordered:
        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("total_return") or 0.0) * 100 for row in ordered], color="#2563eb")
        ax.set_ylabel("After-tax total return (%)")
        ax.set_title("CCEL v1a after-tax total return")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "ccel_total_return")

        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("ulcer_index") or 0.0) * 100 for row in ordered], color="#7c3aed")
        ax.set_ylabel("Ulcer Index (%)")
        ax.set_title("After-tax drawdown persistence")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "ccel_ulcer_index")

        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("max_drawdown") or 0.0) * 100 for row in ordered], color="#dc2626")
        ax.set_ylabel("Max drawdown (%)")
        ax.set_title("After-tax max drawdown")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "ccel_drawdown")

    fig, ax = plt.subplots(figsize=(11.0, 5.2))
    for arm in _report_arm_order():
        payload = results.get(arm)
        if not payload:
            continue
        curve = pd.DataFrame(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [])
        if curve.empty:
            continue
        curve.index = pd.to_datetime(curve["date"])
        equity = pd.to_numeric(curve["equity"], errors="coerce").dropna()
        if not equity.empty:
            ax.plot(equity.index, equity / float(equity.iloc[0]), label=_arm_label(arm), linewidth=1.4)
    ax.set_title("After-tax growth of $1")
    ax.set_ylabel("Multiple")
    ax.legend(loc="best", fontsize=8)
    save(fig, "ccel_equity_curves")
    return charts


def _ccel_report_html(summary: dict[str, Any], charts: dict[str, str], results: dict[str, dict[str, Any]]) -> str:
    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    verdict = dict(summary.get("verdict") or {})
    title = f"CCEL v1a Research Proxy {pd.Timestamp(summary.get('start')).year}-{pd.Timestamp(summary.get('end')).year}"
    status_text = _verdict_status_text(verdict)
    gate_text = "Open" if verdict.get("v1b_research_gate_open") else "Closed"
    risk_text = "Passed" if verdict.get("risk_bar_ok") else "Failed"
    chart_tags = "\n".join(
        f'<figure><img src="assets/{html.escape(Path(path).name)}" alt="{html.escape(name)}"><figcaption>{html.escape(name.replace("_", " ").title())}</figcaption></figure>'
        for name, path in charts.items()
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #111827; margin: 28px; line-height: 1.45; }}
    h1, h2, h3 {{ margin-bottom: 8px; }}
    .banner {{ max-width: 1180px; border: 1px solid #f59e0b; background: #fffbeb; border-radius: 8px; padding: 12px 14px; }}
    .summary {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 22px; max-width: 1120px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px 14px; background: #fff; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: 0; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    .pass {{ color: #047857; }}
    .fail {{ color: #b91c1c; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 1180px; margin: 12px 0 24px; font-size: 13px; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 7px 9px; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, td.text, th.text {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    figure {{ margin: 18px 0; max-width: 1080px; }}
    img {{ max-width: 100%; border: 1px solid #e5e7eb; border-radius: 6px; }}
    figcaption {{ color: #4b5563; font-size: 12px; }}
    .note {{ max-width: 1120px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px 14px; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
	  <p>Generated {html.escape(str(summary.get("generated_at")))} at git SHA <code>{html.escape(str(summary.get("git_sha")))}</code>. Window: <code>{html.escape(str(summary.get("start")))}</code> to <code>{html.escape(str(summary.get("end")))}</code>. OOS starts <code>{html.escape(str(summary.get("oos_start")))}</code>.</p>
	  <div class="banner"><strong>{html.escape(PROXY_LABEL)}.</strong> This run can disprove CCEL, but it cannot certify edge because the basket uses current survivors and lacks point-in-time fundamentals/delisted names.</div>
	  <section class="summary">
	    <div class="card"><div class="label">CCEL status</div><div class="value {'pass' if verdict.get("ccel_v1a_not_killed") else 'fail'}">{html.escape(status_text)}</div></div>
	    <div class="card"><div class="label">V1b research gate</div><div class="value {'pass' if verdict.get("v1b_research_gate_open") else 'fail'}">{html.escape(gate_text)}</div></div>
	    <div class="card"><div class="label">Strict risk bar</div><div class="value {'pass' if verdict.get("risk_bar_ok") else 'fail'}">{html.escape(risk_text)}</div></div>
	    <div class="card"><div class="label">Production defaults changed</div><div class="value fail">No</div></div>
	    <div class="card"><div class="label">Available basket names</div><div class="value">{int(summary.get("available_ticker_count") or 0)} / {int(summary.get("basket_size") or 0)}</div></div>
	  </section>
	  <div class="note"><strong>Executive readout.</strong> {html.escape(str(verdict.get("recommended_next_step") or ""))} This is a research continuation gate, not validation of live edge.</div>
  <h2>Strategy Arms</h2>
  {_html_table(["Arm", "Strategy Used", "Tax / Exit Rule"], _strategy_rows(results), text_columns={0, 1, 2})}
  <h2>After-tax Results</h2>
  {_html_table(["Arm", "Terminal Wealth", "Total Return", "CAGR", "Vol", "Sharpe", "Sortino", "Max DD", "Calmar", "Ulcer", "Turnover", "Costs", "Trades"], _result_rows(rows))}
  <h2>Ablations</h2>
  {_html_table(["Comparison", "Terminal Wealth Delta", "CAGR Delta", "Ulcer Delta", "Interpretation"], _ablation_rows(rows), text_columns={0, 4})}
  <h2>Performance By Year</h2>
  {_html_table(["Year", *[_arm_label(arm) for arm in _report_arm_order()]], _yearly_rows(summary))}
  <h2>Charts</h2>
  {chart_tags}
  <h2>Stress Windows</h2>
  {_html_table(["Arm", "Window", "Return", "Max DD", "Exposure", "Trades"], _stress_table_rows(rows))}
  <h2>Coverage</h2>
  <p>{html.escape(str(summary.get("adjusted_price_note") or ""))}</p>
  {_html_table(["Ticker", "Sector", "First Date", "Last Date", "Rows", "Starts Late"], _coverage_rows(summary))}
  <h2>Limitations</h2>
  <ul>{''.join(f"<li>{html.escape(str(item))}</li>" for item in summary.get("limitations") or [])}</ul>
  <h2>Reproduction</h2>
  <pre>{html.escape(str(summary.get("single_command") or ""))}</pre>
</body>
</html>
"""


def _ccel_row(arm: str, payload: dict[str, Any], result_path: Path) -> dict[str, Any]:
    row = _campaign_row(arm, payload)
    metrics = dict(payload.get("metrics") or {})
    row.update(
        {
            "arm": arm,
            "after_tax_terminal_wealth": metrics.get("after_tax_terminal_wealth"),
            "pre_tax_terminal_wealth": metrics.get("pre_tax_terminal_wealth"),
            "terminal_tax_liability": metrics.get("terminal_tax_liability"),
            "taxes_paid": metrics.get("taxes_paid"),
            "ulcer_index": metrics.get("ulcer_index"),
            "sortino_ratio": metrics.get("sortino_ratio"),
            "trade_count": metrics.get("trade_count"),
            "result_path": str(result_path),
            "proxy_label": payload.get("proxy_label") or PROXY_LABEL,
            "stress_windows": payload.get("stress_windows") or [],
        }
    )
    return row


def _run_reference_result(spec: StrategySpec, frames: dict[str, pd.DataFrame], cfg: PortfolioBacktestConfig, benchmark_curve: pd.DataFrame, windows: list[StressWindow]) -> dict[str, Any]:
    return run_portfolio_backtest(frames, spec, cfg, benchmark_curve=benchmark_curve, windows=windows).to_dict()


def _campaign3_winner_spec(start: str, end: str) -> tuple[str, StrategySpec]:
    specs = campaign3_specs()
    summary_path = DEFAULT_CAMPAIGN_DIR / f"portfolio_campaign3_{pd.Timestamp(start).year}_{pd.Timestamp(end).year}" / "summary.json"
    if summary_path.exists():
        try:
            summary = dict(_read_json(summary_path))
            arm = str((summary.get("verdict") or {}).get("best_l1_candidate_arm") or "")
            if arm in specs:
                return arm, specs[arm]
        except Exception:
            pass
    if "L1_tv15_min40_band40" in specs:
        return "L1_tv15_min40_band40", specs["L1_tv15_min40_band40"]
    return "L1", campaign2_headline_specs()["L1"]


def _load_results(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for row in summary.get("rows") or []:
        if not isinstance(row, dict):
            continue
        arm = str(row.get("arm") or "")
        path = Path(str(row.get("result_path") or ""))
        if arm and path.exists():
            results[arm] = dict(_read_json(path))
    return results


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    rename = {"Open": "open", "High": "high", "Low": "low", "Close": "price", "Adj Close": "price", "Volume": "volume"}
    normalized = normalized.rename(columns={column: rename.get(str(column), str(column)) for column in normalized.columns})
    if "price" not in normalized.columns and "close" in normalized.columns:
        normalized["price"] = normalized["close"]
    if "open" not in normalized.columns:
        normalized["open"] = normalized["price"]
    normalized["price"] = pd.to_numeric(normalized["price"], errors="coerce")
    normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
    return normalized.sort_index().dropna(subset=["price", "open"])


def _panel_dates(frames: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    dates: set[pd.Timestamp] = set()
    for frame in frames.values():
        dates.update(pd.Timestamp(index).normalize() for index in frame.index)
    return sorted(dates)


def _date_text(value: Any) -> str:
    return str(pd.Timestamp(value).date().isoformat())


def _position_value(lots: list[CCELLot], prices: dict[str, float]) -> float:
    return float(sum(lot.quantity * float(prices.get(lot.ticker, 0.0)) for lot in lots))


def _held_tickers(lots: list[CCELLot]) -> set[str]:
    return {lot.ticker for lot in lots if lot.quantity > 0}


def _unique_tickers(tickers: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in tickers:
        ticker = str(raw or "").upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def _held_value_weights(tickers: list[str], lots: list[CCELLot], prices: dict[str, float]) -> dict[str, float]:
    values: dict[str, float] = {}
    for ticker in _unique_tickers(tickers):
        qty = sum(lot.quantity for lot in lots if lot.ticker == ticker)
        price = float(prices.get(ticker, 0.0))
        if qty > 0 and price > 0:
            values[ticker] = qty * price
    total = sum(values.values())
    if total <= 0:
        return {ticker: 1.0 for ticker in _unique_tickers(tickers)}
    return {ticker: value / total for ticker, value in values.items()}


def _weighted_basis(lots: list[CCELLot]) -> float:
    total_qty = sum(lot.quantity for lot in lots)
    if total_qty <= 0:
        return 0.0
    return sum(lot.quantity * lot.basis_per_share for lot in lots) / total_qty


def _momentum_12_1(ticker: str, date: pd.Timestamp, frames: dict[str, pd.DataFrame]) -> float | None:
    frame = frames.get(ticker)
    if frame is None or frame.empty:
        return None
    rows = frame.loc[frame.index <= pd.Timestamp(date)]
    if len(rows) < 253:
        return None
    price = pd.to_numeric(rows["price"], errors="coerce")
    if float(price.iloc[-252]) <= 0:
        return None
    return float(price.iloc[-21] / price.iloc[-252] - 1.0)


def _bottom_momentum(ticker: str, date: pd.Timestamp, frames: dict[str, pd.DataFrame], active: set[str], quantile: float) -> bool:
    scored: list[tuple[str, float]] = []
    for name in active:
        score = _momentum_12_1(name, date, frames)
        if score is not None:
            scored.append((name, score))
    if len(scored) < 4:
        return False
    scored.sort(key=lambda item: item[1])
    cutoff = max(1, math.ceil(len(scored) * max(0.0, min(1.0, quantile))))
    return ticker in {name for name, _score in scored[:cutoff]}


def _quality_fails(ticker: str, date: pd.Timestamp, frames: dict[str, pd.DataFrame]) -> bool:
    frame = frames.get(ticker)
    if frame is None or frame.empty:
        return False
    rows = frame.loc[frame.index <= pd.Timestamp(date)]
    if rows.empty:
        return False
    row = rows.iloc[-1]
    if "quality_gate_pass" in row:
        return not bool(row.get("quality_gate_pass"))
    if "quality_score" in row:
        value = _float(row.get("quality_score"))
        return value is not None and value < 0.2
    return False


def _is_first_trading_day_month(date: pd.Timestamp, frames: dict[str, pd.DataFrame]) -> bool:
    current = pd.Timestamp(date)
    prior_dates = [idx for frame in frames.values() for idx in frame.index if idx < current]
    if not prior_dates:
        return True
    return bool(pd.Timestamp(max(prior_dates)).month != current.month)


def _apply_carry_to_gain(gain: float, carry: float) -> tuple[float, float]:
    if gain <= 0 or carry >= 0:
        return gain, carry
    offset = min(gain, abs(carry))
    return gain - offset, carry + offset


def _net_st_lt(st: float, lt: float) -> tuple[float, float]:
    if st > 0 and lt < 0:
        offset = min(st, abs(lt))
        return st - offset, lt + offset
    if lt > 0 and st < 0:
        offset = min(lt, abs(st))
        return st + offset, lt - offset
    return st, lt


def _calmar(metrics: dict[str, Any]) -> float | None:
    annual = _float(metrics.get("annualized_return"))
    drawdown = abs(_float(metrics.get("max_drawdown")) or 0.0)
    if annual is None or drawdown <= 0:
        return None
    return annual / drawdown


def _annualized_turnover(total_turnover: float, rows: int) -> float:
    if rows <= 1:
        return 0.0
    return float(total_turnover) * 252.0 / rows


def _stable_hash(payload: dict[str, Any]) -> str:
    import hashlib
    import json

    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def _replace_config(config: CCELConfig, **updates: Any) -> CCELConfig:
    payload = config.to_dict()
    payload.update(updates)
    return CCELConfig(**payload)


def _campaign_metadata(*, arm: str, start: str, end: str, oos_start: str, kind: str) -> dict[str, Any]:
    return {
        "campaign": "ccel_v1a",
        "arm": arm,
        "kind": kind,
        "git_sha": _git_sha(),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "start": start,
        "end": end,
        "oos_start": oos_start,
        "proxy_label": PROXY_LABEL,
        "research_only": True,
    }


def _row_order(row: dict[str, Any]) -> tuple[int, str]:
    order = {arm: idx for idx, arm in enumerate(_report_arm_order())}
    return (order.get(str(row.get("arm")), 999), str(row.get("arm")))


def _report_arm_order() -> list[str]:
    return ["CCEL_v1a", "CCEL_no_harvest", "CCEL_core_only", "CCEL_core_only_no_harvest", "L1", "Campaign3_winner", "QQQ_buy_hold", "SPY_buy_hold"]


def _arm_label(arm: str) -> str:
    labels = {
        "CCEL_v1a": "CCEL v1a",
        "CCEL_no_harvest": "CCEL no harvest",
        "CCEL_core_only": "CCEL core-only",
        "CCEL_core_only_no_harvest": "CCEL core/no harvest",
        "L1": "L1",
        "Campaign3_winner": "Campaign 3 winner",
        "QQQ_buy_hold": "QQQ buy-hold",
        "SPY_buy_hold": "SPY buy-hold",
    }
    return labels.get(arm, arm)


def _verdict_status_text(verdict: dict[str, Any]) -> str:
    status = str(verdict.get("validation_status") or "")
    if status == "proxy_bar_cleared_not_validated":
        return "Proxy bar"
    if status == "not_killed_not_validated":
        return "Not killed"
    if status == "killed":
        return "Killed"
    return "Research"


def _strategy_rows(results: dict[str, dict[str, Any]]) -> list[list[Any]]:
    descriptions = {
        "CCEL_v1a": ["Buy-only deployment into the current campaign basket; equal-weight at entry; winners drift and are not trimmed.", "FIFO lots, ST/LT rates 32%/20%, wash-sale loss deferral, monthly harvesting, probation exits. Harvest proceeds use exposure-neutral replacement by default."],
        "CCEL_no_harvest": ["Same as CCEL v1a, but the explicit loss-harvesting rule is disabled.", "Taxes still apply to any probation/fundamental exits. This isolates harvest timing from the rest of the management layer."],
        "CCEL_core_only": ["All positions are treated as core; probation exits are disabled while exposure-neutral harvesting remains active.", "Tests whether probation logic adds value beyond core compounding plus harvests."],
        "CCEL_core_only_no_harvest": ["Core-only hold logic with no explicit harvesting.", "Lowest-turnover CCEL proxy ablation."],
        "L1": ["Campaign 2 L1 volatility-targeted equal-weight basket.", "Post-processed through the same FIFO/wash/annual-tax research model."],
        "Campaign3_winner": [f"Best L1 candidate from Campaign 3 where available: {results.get('Campaign3_winner', {}).get('campaign3_winner_arm', 'unknown')}.", "Post-processed through the same research tax model."],
        "QQQ_buy_hold": ["QQQ buy-and-hold benchmark.", "Terminal liquidation tax applied at the end of the window."],
        "SPY_buy_hold": ["SPY buy-and-hold benchmark.", "Terminal liquidation tax applied at the end of the window."],
    }
    return [[_arm_label(arm), *(descriptions.get(arm) or ["", ""])] for arm in _report_arm_order() if arm in results]


def _result_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    by_arm = {str(row.get("arm")): row for row in rows}
    output: list[list[Any]] = []
    for arm in _report_arm_order():
        row = by_arm.get(arm)
        if not row:
            continue
        output.append(
            [
                _arm_label(arm),
                _fmt_num(row.get("after_tax_terminal_wealth")),
                _fmt_pct(row.get("total_return")),
                _fmt_pct(row.get("annualized_return")),
                _fmt_pct(row.get("annualized_volatility")),
                _fmt_num(row.get("sharpe_ratio")),
                _fmt_num(row.get("sortino_ratio")),
                _fmt_pct(row.get("max_drawdown")),
                _fmt_num(row.get("calmar_ratio")),
                _fmt_pct(row.get("ulcer_index")),
                _fmt_num(row.get("annualized_turnover")),
                _fmt_num(row.get("total_costs_paid")),
                _fmt_num(row.get("trade_count")),
            ]
        )
    return output


def _ablation_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    by_arm = {str(row.get("arm")): row for row in rows}
    pairs = [
        ("Exposure-neutral harvesting", "CCEL_v1a", "CCEL_no_harvest", "Positive means the harvest rule plus wash-safe replacement helped; this is not standalone tax alpha."),
        ("Probation sleeve", "CCEL_v1a", "CCEL_core_only", "Positive means probation logic helped."),
        ("Core exposure-neutral harvest", "CCEL_core_only", "CCEL_core_only_no_harvest", "Positive means core-level harvest replacement helped without momentum-ranked new-name rotation."),
    ]
    output = []
    for label, a, b, note in pairs:
        left = by_arm.get(a) or {}
        right = by_arm.get(b) or {}
        output.append(
            [
                label,
                _fmt_num((_float(left.get("after_tax_terminal_wealth")) or 0.0) - (_float(right.get("after_tax_terminal_wealth")) or 0.0)),
                _fmt_pct((_float(left.get("annualized_return")) or 0.0) - (_float(right.get("annualized_return")) or 0.0)),
                _fmt_pct((_float(left.get("ulcer_index")) or 0.0) - (_float(right.get("ulcer_index")) or 0.0)),
                note,
            ]
        )
    return output


def _yearly_rows(summary: dict[str, Any]) -> list[list[Any]]:
    by_year: dict[int, dict[str, Any]] = {}
    for arm in _report_arm_order():
        for row in (summary.get("yearly_returns") or {}).get(arm) or []:
            if not isinstance(row, dict):
                continue
            year = int(pd.Timestamp(row.get("period")).year)
            by_year.setdefault(year, {})[arm] = row.get("return")
    return [[year, *[_fmt_pct(values.get(arm)) if arm in values else "" for arm in _report_arm_order()]] for year, values in sorted(by_year.items())]


def _stress_table_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    output: list[list[Any]] = []
    for row in rows:
        for item in row.get("stress_windows") or []:
            if not isinstance(item, dict):
                continue
            output.append(
                [
                    _arm_label(str(row.get("arm"))),
                    item.get("label") or item.get("key"),
                    _fmt_pct(item.get("strategy_total_return")),
                    _fmt_pct(item.get("strategy_max_drawdown")),
                    _fmt_pct(item.get("exposure_mean")),
                    _fmt_num(item.get("trade_count")),
                ]
            )
    return output


def _coverage_rows(summary: dict[str, Any]) -> list[list[Any]]:
    return [
        [row.get("ticker"), row.get("sector"), row.get("first_date"), row.get("last_date"), row.get("row_count"), "yes" if row.get("starts_after_target") else "no"]
        for row in ((summary.get("coverage") or {}).get("tickers") or [])
    ]


def _append_run_log(root: Path, line: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).isoformat()
    path = root / "run_log.md"
    existing = path.read_text(encoding="utf-8") if path.exists() else "# CCEL v1a run log\n\n"
    path.write_text(existing.rstrip() + f"\n- {stamp}: {line}\n", encoding="utf-8")


def _load_default_frame(ticker: str, start: str, end: str) -> pd.DataFrame:
    end_exclusive = (pd.Timestamp(end) + pd.Timedelta(days=1)).date().isoformat()
    return download_market_frame(ticker=ticker, period="max", interval="1d", start=start, end=end_exclusive, cache=True).frame
