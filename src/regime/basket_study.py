from __future__ import annotations

import datetime as dt
import gzip
import html
import json
import math
import os
import re
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Sequence

import pandas as pd

from .alpha_campaign import DEFAULT_BASKET_PATH, DEFAULT_CAMPAIGN_DIR, _git_sha, _json_safe, _read_json, _write_json, load_basket
from .ccel_campaign import (
    CCELLot,
    apply_wash_sales,
    build_after_tax_curve,
    buy_hold_taxable_payload,
    reconstruct_lots_from_trades,
    _annualized_turnover,
    _date_text,
    _metrics,
    _normalize_frame,
    _sell_fifo_lots,
    _stable_hash,
)
from .portfolio_historical_campaign import _benchmark_relative_rows, _html_table, _period_returns, _stress_results_for_curve, historical_stress_windows_for_range
from .sharadar import DEFAULT_SHARADAR_DIR, SharadarStore, certification_gate_status, classify_readiness
from .stress_windows import StressWindow
from .sharadar.store import TERMINAL_DEFAULTS_ARTIFACT_NAME

DEFAULT_BASKET_STUDY_START = "2006-01-01"
DEFAULT_BASKET_STUDY_END = "2025-12-31"
DEFAULT_BASKET_STUDY_OOS_START = "2024-01-01"
DEFAULT_BASKET_STUDY_DIR = DEFAULT_CAMPAIGN_DIR / "basket_construction_study"
DEFAULT_BASKET_STUDY_REPORT_DIR = Path("output") / "basket_construction_study_report"
DEFAULT_BASKET_STUDY_REPORT_PATH = DEFAULT_BASKET_STUDY_REPORT_DIR / "management_report.html"

SELECTION_ARMS = (
    "C0_static_basket",
    "C0b_static_pit",
    "A1_pure_momentum",
    "A2_quality_momentum",
    "A3_momentum_valuation_cap",
    "A4_quality_momentum_valuation",
)
BENCHMARK_ARMS = ("static_basket_equal_weight", "SPY_buy_hold", "L1_vol_target")
TERMINAL_POLICY_COMPARISON_ARMS = (
    "C0b_static_pit",
    "A1_pure_momentum",
    "A2_quality_momentum",
    "A3_momentum_valuation_cap",
    "A4_quality_momentum_valuation",
    "SPY_buy_hold",
)
TERMINAL_LAST_PRICE_SOURCES = {"acquisition_last_price", "unknown_healthy_last_price"}
TERMINAL_FAILURE_ZERO_SOURCES = {
    "actions_failure_default_zero",
    "metadata_failure_default_zero",
    "seeded_failure_default_zero",
    "unknown_distressed_zero",
    "unknown_missing_price_zero",
    "acquisition_missing_price_zero",
}
TERMINAL_UNKNOWN_RESIDUAL_SOURCES = {"unknown_healthy_last_price", "unknown_distressed_zero", "unknown_missing_price_zero"}
QUALITY_FIELDS = (
    "revenue",
    "assets",
    "liabilities",
    "ebit",
    "taxexp",
    "debt",
    "equity",
    "fcf",
    "ncfo",
    "capex",
    "gp",
    "ebitda",
    "netinc",
)
_CANDIDATE_FEATURE_CACHE: dict[tuple[Any, ...], tuple[SelectionRow, ...]] = {}


@dataclass(frozen=True)
class BasketStudyConfig:
    starting_cash: float = 100_000.0
    basket_size: int = 12
    weighting: Literal["equal_weight_at_entry", "equal_weight_rebalanced"] = "equal_weight_at_entry"
    reconstitution: Literal["drop_bottom_third", "full_reselect"] = "drop_bottom_third"
    formation: Literal["12_1", "6_1"] = "12_1"
    min_dollar_adv: float = 10_000_000.0
    min_marketcap: float = 500_000_000.0
    min_listing_days: int = 252
    dollar_adv_days: int = 63
    annual_reconstitution_month: int = 1
    annual_reconstitution_day: int = 2
    entry_cost_bps: float = 5.0
    exit_cost_bps: float = 5.0
    st_tax_rate: float = 0.32
    lt_tax_rate: float = 0.20
    significant_gain_pct: float = 0.10
    universe_top_n: int = 750
    expensive_decile: float = 0.90
    oos_start: str = DEFAULT_BASKET_STUDY_OOS_START

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SelectionRow:
    permaticker: int
    ticker: str
    score: float
    momentum: float | None
    quality: float | None
    valuation: float | None
    marketcap: float | None
    dollar_adv: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_basket_study(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_BASKET_STUDY_DIR,
    report_dir: str | Path = DEFAULT_BASKET_STUDY_REPORT_DIR,
    store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    start: str = DEFAULT_BASKET_STUDY_START,
    end: str = DEFAULT_BASKET_STUDY_END,
    oos_start: str = DEFAULT_BASKET_STUDY_OOS_START,
    resume: bool = False,
    render_report: bool = True,
    config: BasketStudyConfig | None = None,
) -> dict[str, Any]:
    started = dt.datetime.now(dt.timezone.utc)
    root = Path(campaign_dir)
    previous_summary = _safe_read_json(root / "summary.json")
    result_dir = root / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    cfg = config or BasketStudyConfig(oos_start=oos_start)
    store = SharadarStore(store_dir)
    if not store.exists():
        raise ValueError(f"Sharadar store not found: {store_dir}")
    terminal_defaults_artifact = store.refresh_terminal_value_defaults_artifact()

    synth_sp500 = store.synth_sp500_total_return(start, end)
    if synth_sp500.empty:
        raise ValueError("Synthesized S&P 500 benchmark is unavailable from Sharadar SP500, SEP, and DAILY.")
    benchmark_curve = _buy_hold_curve(synth_sp500, starting_cash=cfg.starting_cash)
    stress_windows = historical_stress_windows_for_range(start, end)
    basket = load_basket(basket_path) if Path(basket_path).exists() else {"tickers": []}

    results: dict[str, dict[str, Any]] = {}
    for arm in SELECTION_ARMS:
        path = result_dir / f"{_safe_name(arm)}.json"
        if resume and path.exists():
            payload = dict(_read_json(path))
        else:
            payload = run_basket_arm(store, arm, cfg, start=start, end=end, basket=basket, benchmark_curve=benchmark_curve, windows=stress_windows)
            _write_json(path, payload)
        results[arm] = payload

    benchmark_payloads = {
        "static_basket_equal_weight": run_basket_arm(
            store,
            "static_basket_equal_weight",
            BasketStudyConfig(**{**cfg.to_dict(), "weighting": "equal_weight_rebalanced"}),
            start=start,
            end=end,
            basket=basket,
            benchmark_curve=benchmark_curve,
            windows=stress_windows,
        ),
        "SPY_buy_hold": _stamp_benchmark_payload(
            buy_hold_taxable_payload("SYNTH_SP500", synth_sp500, oos_start=oos_start, benchmark_curve=benchmark_curve, windows=stress_windows),
            cfg,
            "SPY_buy_hold",
            benchmark_source="synth_sp500_total_return",
        ),
        "L1_vol_target": run_vol_target_benchmark(synth_sp500, cfg, benchmark_curve=benchmark_curve, windows=stress_windows),
    }
    for arm, payload in benchmark_payloads.items():
        path = result_dir / f"{_safe_name(arm)}.json"
        if not (resume and path.exists()):
            _write_json(path, payload)
        else:
            payload = dict(_read_json(path))
        results[arm] = payload

    rows = [_study_row(arm, payload, result_dir / f"{_safe_name(arm)}.json") for arm, payload in results.items()]
    rows = sorted(rows, key=lambda row: _arm_sort_key(str(row.get("arm") or "")))
    readiness = classify_readiness(store, _readiness_identifiers(store, results), (start, end)).to_dict()
    edgar = load_edgar_validation_artifact(store_dir)
    gate = basket_study_gate_status(readiness, edgar, oos_start=oos_start)
    arm_verdicts = basket_study_arm_verdicts(rows, gate_status=gate)
    verdict = basket_study_verdict(rows, gate_status=gate, readiness=readiness, edgar_artifact=edgar, arm_verdicts=arm_verdicts)
    survivorship_delta = survivorship_bias_delta(rows)
    terminal_event_use_counts = basket_study_terminal_event_use_counts(results, store)
    terminal_value_disclosure = terminal_value_disclosure_summary(terminal_event_use_counts)
    terminal_policy_comparison = terminal_value_policy_comparison(
        previous_summary,
        rows,
        terminal_event_use_counts,
        current_snapshot_hash=store.data_snapshot_hash,
    )
    finished = dt.datetime.now(dt.timezone.utc)
    monthly_returns = {arm: _period_returns(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [], "M") for arm, payload in results.items()}
    yearly_returns = {arm: _period_returns(payload.get("after_tax_equity_curve") or payload.get("equity_curve") or [], "Y") for arm, payload in results.items()}
    summary = {
        "schema": "regime_basket_construction_study.v1",
        "generated_at": finished.isoformat(),
        "git_sha": _git_sha(),
        "wall_clock_seconds": (finished - started).total_seconds(),
        "start": start,
        "end": end,
        "oos_start": oos_start,
        "basket_path": str(basket_path),
        "config": cfg.to_dict(),
        "arms": list(SELECTION_ARMS),
        "benchmarks": list(BENCHMARK_ARMS),
        "rows": rows,
        "survivorship_bias_delta": survivorship_delta,
        "benchmark_relative": basket_study_benchmark_relative_rows(rows),
        "terminal_event_use_counts": terminal_event_use_counts,
        "terminal_value_disclosure": terminal_value_disclosure,
        "terminal_policy_comparison": terminal_policy_comparison,
        "terminal_value_defaults_artifact": str(Path(store_dir) / TERMINAL_DEFAULTS_ARTIFACT_NAME),
        "terminal_value_defaults_artifact_hash": terminal_defaults_artifact.get("artifact_hash"),
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "readiness": readiness,
        "data_readiness": readiness.get("data_readiness"),
        "data_snapshot_hash": store.data_snapshot_hash,
        "edgar_validation": edgar,
        "gate_status": gate,
        "arm_verdicts": arm_verdicts,
        "verdict": verdict,
        "production_defaults_changed": False,
        "single_command": (
            "python -m src.regime.cli basket-study run "
            f"--start {start} --end {end} --oos-start {oos_start} --store-dir {Path(store_dir)} "
            f"--basket {Path(basket_path)} --campaign-dir {root} --report-dir {Path(report_dir)}"
        ),
        "limitations": [
            "Research-only. No production trading defaults changed.",
            "Certifiable status requires survivorship_free readiness and a PASSING EDGAR validation artifact tied to the same data snapshot hash.",
            "The passive index benchmark is a synthesized cap-weighted S&P 500 total-return proxy built from Sharadar SP500 membership, SEP adjusted prices, and DAILY market caps.",
            "Selection uses local Sharadar PIT data only; no yfinance fallback is allowed.",
            "Readiness separates survivorship coverage from fundamental scoreability: missing SF1 fundamentals are documented exceptions when adjusted prices and terminal value handling are present.",
            "Delisted-name terminal values are reason-dependent: failures mark to $0, acquisition-like delistings mark to the last traded adjusted SEP price, and unknown residuals branch by pre-delisting health with disclosure.",
        ],
    }
    _write_json(root / "summary.json", summary)
    if render_report:
        report_path = render_basket_study_report(campaign_dir=root, output_dir=report_dir)
        summary["report_path"] = str(report_path)
        _write_json(root / "summary.json", summary)
    return summary


def run_basket_arm(
    store: SharadarStore,
    arm: str,
    config: BasketStudyConfig,
    *,
    start: str,
    end: str,
    basket: dict[str, Any] | None = None,
    benchmark_curve: pd.DataFrame | None = None,
    windows: list[StressWindow] | None = None,
) -> dict[str, Any]:
    cfg = config
    selections = annual_selection_schedule(store, arm, cfg, start=start, end=end, basket=basket or {})
    needed = sorted({int(row.permaticker) for rows in selections.values() for row in rows})
    if not needed:
        raise ValueError(f"No selectable names for basket study arm {arm}.")
    prices = store.get_prices(needed, start, end)
    prices = {perma: _normalize_frame(frame) for perma, frame in prices.items() if not frame.empty}
    if not prices:
        raise ValueError(f"No price history for basket study arm {arm}.")
    terminal_events = store.terminal_value_events(needed, start=start, end=end)
    dates = _panel_dates_by_permaticker(prices)
    dates = sorted(set(dates) | {event.date for event in terminal_events.values()})
    cash = float(cfg.starting_cash)
    lots: list[CCELLot] = []
    trades: list[dict[str, Any]] = []
    realizations: list[dict[str, Any]] = []
    terminal_realizations: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    next_lot_id = 1
    total_costs = 0.0
    total_turnover = 0.0
    last_prices: dict[int, float] = {}
    target_holdings: set[int] = set()
    label_by_perma = {perma: store.ticker_for_permaticker(perma, as_of_date=end) for perma in needed}
    executed_selection_dates: set[str] = set()
    processed_terminal_events: set[int] = set()

    for date in dates:
        active_prices = {int(perma): float(frame.loc[date, "open"]) for perma, frame in prices.items() if date in frame.index}
        close_prices = {int(perma): float(frame.loc[date, "price"]) for perma, frame in prices.items() if date in frame.index}
        last_prices.update({int(perma): price for perma, price in close_prices.items()})
        cash, terminal_trades, terminal_realized = _execute_terminal_value_events(
            date=date,
            lots=lots,
            cash=cash,
            terminal_events=terminal_events,
            processed=processed_terminal_events,
        )
        if terminal_trades:
            trades.extend(terminal_trades)
            realizations.extend(terminal_realized)
            terminal_realizations.extend(terminal_realized)
            for trade in terminal_trades:
                perma = _security_to_perma(str(trade.get("ticker") or "0"))
                target_holdings.discard(perma)
                last_prices[perma] = float(trade.get("price") or 0.0)
        selection_date = _matching_selection_date(selections, date, executed_selection_dates)
        if selection_date is not None:
            executed_selection_dates.add(selection_date)
            ranked = selections[selection_date]
            target_holdings = reconstitute_holdings(
                current={_security_to_perma(lot.ticker) for lot in lots},
                ranked=[row.permaticker for row in ranked],
                scores={row.permaticker: row.score for row in ranked},
                target_size=cfg.basket_size,
                method=cfg.reconstitution,
            )
            target_holdings.difference_update(processed_terminal_events)
            cash, next_lot_id, new_trades, new_realized, costs, turnover = _rebalance_to_targets(
                date=date,
                target_holdings=target_holdings,
                lots=lots,
                cash=cash,
                open_prices=active_prices,
                cfg=cfg,
                next_lot_id=next_lot_id,
                rebalance_weights=(cfg.weighting == "equal_weight_rebalanced"),
            )
            trades.extend(new_trades)
            realizations.extend(new_realized)
            total_costs += costs
            total_turnover += turnover

        valued_prices, unresolved_marks = _mark_prices_for_lots(lots, last_prices)
        position_value = float(sum(lot.quantity * float(valued_prices.get(lot.ticker, 0.0)) for lot in lots))
        equity = cash + position_value
        equity_curve.append(
            {
                "date": _date_text(date),
                "equity": equity,
                "cash": cash,
                "position_value": position_value,
                "exposure": position_value / equity if equity > 0 else 0.0,
                "costs_paid": 0.0,
                "turnover": 0.0,
                "open_lot_count": len(lots),
                "unresolved_mark_count": len(unresolved_marks),
                "zero_mark_count": sum(1 for lot in lots if valued_prices.get(lot.ticker, 0.0) <= 0.0),
            }
        )

    taxable = apply_wash_sales(realizations, trades)
    after_tax_curve, tax_summary = build_after_tax_curve(
        equity_curve,
        taxable,
        lots,
        {lot.ticker: float(_mark_price_for_security(last_prices, lot.ticker) or 0.0) for lot in lots},
        st_tax_rate=cfg.st_tax_rate,
        lt_tax_rate=cfg.lt_tax_rate,
    )
    metrics = _metrics(after_tax_curve, trades, benchmark_curve=benchmark_curve)
    pre_tax_metrics = _metrics(equity_curve, trades, benchmark_curve=benchmark_curve)
    metrics["annualized_turnover"] = _annualized_turnover(total_turnover, len(equity_curve))
    metrics["total_turnover"] = total_turnover
    metrics["total_costs_paid"] = total_costs
    metrics["after_tax_terminal_wealth"] = after_tax_curve[-1]["equity"] if after_tax_curve else None
    metrics["pre_tax_terminal_wealth"] = equity_curve[-1]["equity"] if equity_curve else None
    metrics["terminal_tax_liability"] = tax_summary.get("terminal_tax_liability")
    metrics["taxes_paid"] = tax_summary.get("taxes_paid")
    valuation_diagnostics = _valuation_diagnostics(lots, last_prices)
    metrics.update(valuation_diagnostics)
    metrics.update(per_name_distribution(trades, lots, {_perma_security(perma): price for perma, price in last_prices.items()}, label_by_perma))
    oos_start = pd.Timestamp(cfg.oos_start) if cfg.oos_start else None
    return {
        "schema": "regime_basket_study_arm.v1",
        "arm": arm,
        "config": cfg.to_dict(),
        "strategy_hash": _stable_hash({"arm": arm, "config": cfg.to_dict()}),
        "git_sha": _git_sha(),
        "metrics": _json_safe(metrics),
        "pre_tax_metrics": _json_safe(pre_tax_metrics),
        "in_sample": _json_safe(_segment_metrics(after_tax_curve, trades, benchmark_curve, None, oos_start)),
        "out_of_sample": _json_safe(_segment_metrics(after_tax_curve, trades, benchmark_curve, oos_start, None) if oos_start is not None else None),
        "selection_history": {
            date: [row.to_dict() for row in rows[: cfg.basket_size]]
            for date, rows in selections.items()
        },
        "equity_curve": _json_safe(equity_curve),
        "after_tax_equity_curve": _json_safe(after_tax_curve),
        "trades": _json_safe(trades),
        "realized_lots": _json_safe(taxable),
        "terminal_realizations": _json_safe(terminal_realizations),
        "terminal_events": _json_safe([event.to_dict() for event in terminal_events.values()]),
        "tax_summary": _json_safe(tax_summary),
        "open_lots": _json_safe([lot.to_dict() for lot in lots]),
        "valuation_diagnostics": _json_safe(valuation_diagnostics),
        "stress_windows": _json_safe(_stress_results_for_curve(pd.DataFrame(after_tax_curve), trades, benchmark_curve if benchmark_curve is not None else pd.DataFrame(), windows or [])),
        "production_defaults_changed": False,
    }


def annual_selection_schedule(
    store: SharadarStore,
    arm: str,
    config: BasketStudyConfig,
    *,
    start: str,
    end: str,
    basket: dict[str, Any],
) -> dict[str, list[SelectionRow]]:
    out: dict[str, list[SelectionRow]] = {}
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    for year in range(start_ts.year, end_ts.year + 1):
        as_of = pd.Timestamp(year=year, month=config.annual_reconstitution_month, day=config.annual_reconstitution_day)
        as_of = min(max(as_of, start_ts), end_ts)
        rows = select_basket_asof(store, arm, as_of, config, basket=basket)
        if rows:
            out[_date_text(as_of)] = rows
    return out


def select_basket_asof(
    store: SharadarStore,
    arm: str,
    as_of: str | pd.Timestamp,
    config: BasketStudyConfig,
    *,
    basket: dict[str, Any] | None = None,
) -> list[SelectionRow]:
    date = pd.Timestamp(as_of).normalize()
    if arm in {"C0_static_basket", "static_basket_equal_weight"}:
        return _static_basket_rows(store, date, config, basket or {})
    candidates = _candidate_features(store, date, config)
    if not candidates:
        return []
    if arm == "C0b_static_pit":
        ranked = sorted(candidates, key=lambda row: (_none_low(row.dollar_adv), _none_low(row.marketcap)), reverse=True)
        return [row for idx, row in enumerate(ranked) if idx < max(config.basket_size, 1)]
    scored = _score_selection_rows(candidates, arm, config)
    return sorted(scored, key=lambda row: row.score, reverse=True)


def reconstitute_holdings(
    *,
    current: set[int],
    ranked: Sequence[int],
    scores: dict[int, float],
    target_size: int,
    method: Literal["drop_bottom_third", "full_reselect"] = "drop_bottom_third",
) -> set[int]:
    ranked_unique = [int(item) for item in dict.fromkeys(ranked)]
    if method == "full_reselect" or not current:
        return set(ranked_unique[:target_size])
    current_ranked = [item for item in current if item in scores]
    missing = [item for item in current if item not in scores]
    current_ranked.sort(key=lambda item: scores.get(item, -math.inf))
    drop_count = max(1, math.ceil(len(current) / 3.0)) if current else 0
    dropped = set(current_ranked[:drop_count]) | set(missing)
    retained = set(current) - dropped
    target = set(retained)
    for item in ranked_unique:
        if len(target) >= target_size:
            break
        target.add(item)
    return target


def validate_edgar_sample(
    *,
    store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    output_path: str | Path | None = None,
    sample_size: int = 20,
    validator: Callable[[SharadarStore, list[int]], list[dict[str, Any]]] | None = None,
    allow_network: bool = True,
) -> dict[str, Any]:
    """Write an EDGAR validation artifact bound to the current snapshot hash.

    The default path is intentionally conservative: without an injected EDGAR
    validator the artifact is a FAIL/manual-required record. That prevents any
    study from accidentally becoming certifiable before independent SEC filing
    tie-out has actually been performed.
    """

    store = SharadarStore(store_dir)
    store.refresh_terminal_value_defaults_artifact()
    candidate_size = max(sample_size * 8, sample_size)
    sample = _edgar_sample_permatickers(store, candidate_size)
    attempted_failures: list[dict[str, Any]] = []
    if validator is not None:
        rows = validator(store, sample[:sample_size])
    elif allow_network:
        rows, attempted_failures = _sec_companyfacts_sample_validation(store, sample, target_passes=sample_size)
    else:
        rows = [
            {
                "permaticker": perma,
                "ticker": store.ticker_for_permaticker(perma),
                "status": "not_checked",
                "reason": "EDGAR network validation disabled",
            }
            for perma in sample
        ]
        rows = rows[:sample_size]
    passed = bool(rows) and all(str(row.get("status") or "").lower() == "pass" for row in rows)
    artifact = {
        "schema": "regime_sharadar_edgar_validation.v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data_snapshot_hash": store.data_snapshot_hash,
        "sample_size": len(rows),
        "candidate_count": len(sample),
        "sample_strategy": "weighted_delisted_small_cap_common_stocks_with_sec_cik",
        "status": "PASS" if passed else "FAIL",
        "rows": rows,
        "attempted_failures": attempted_failures[: max(sample_size, 20)],
        "production_defaults_changed": False,
    }
    path = Path(output_path) if output_path is not None else edgar_validation_artifact_path(store_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(artifact), indent=2, sort_keys=True), encoding="utf-8")
    return artifact


def _sec_companyfacts_validator(store: SharadarStore, sample: list[int]) -> list[dict[str, Any]]:
    return [_sec_companyfacts_validation_row(store, perma) for perma in sample]


def _sec_companyfacts_sample_validation(
    store: SharadarStore,
    sample: list[int],
    *,
    target_passes: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    passes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    seen: set[int] = set()
    for perma in sample:
        if perma in seen:
            continue
        seen.add(perma)
        row = _sec_companyfacts_validation_row(store, perma)
        if str(row.get("status") or "").lower() == "pass":
            passes.append(row)
            if len(passes) >= target_passes:
                break
        else:
            failures.append(row)
    return passes, failures


def _sec_companyfacts_validation_row(store: SharadarStore, perma: int) -> dict[str, Any]:
    tickers = store.read_meta_table("TICKERS")
    ticker = store.ticker_for_permaticker(perma)
    try:
        sf1 = _latest_sf1_row_for_validation(store, perma)
        if not sf1:
            return {"permaticker": perma, "ticker": ticker, "status": "fail", "reason": "sf1_row_unavailable"}
        cik = _cik_for_permaticker(tickers, perma)
        if cik is None:
            return {"permaticker": perma, "ticker": ticker, "status": "fail", "reason": "cik_unavailable", "sf1": sf1}
        facts = _companyfacts_json(cik)
        checks = _edgar_fact_checks(sf1, facts)
        status = "pass" if checks and all(bool(item.get("passed")) for item in checks) else "fail"
        return {
            "permaticker": perma,
            "ticker": ticker,
            "cik": cik,
            "status": status,
            "sf1_datekey": sf1.get("datekey"),
            "sf1_calendardate": sf1.get("calendardate"),
            "checks": checks,
        }
    except Exception as exc:
        return {"permaticker": perma, "ticker": ticker, "status": "fail", "reason": str(exc)}


def _latest_sf1_row_for_validation(store: SharadarStore, permaticker: int) -> dict[str, Any] | None:
    fields = [*QUALITY_FIELDS, "calendardate"]
    payload = store.get_fundamentals_asof(permaticker, pd.Timestamp.today(), fields)
    return dict(payload) if payload else None


def _cik_for_permaticker(tickers: pd.DataFrame, permaticker: int) -> int | None:
    if tickers.empty or "permaticker" not in tickers.columns:
        return None
    rows = tickers.loc[pd.to_numeric(tickers["permaticker"], errors="coerce") == int(permaticker)].copy()
    if "secfilings" in rows.columns:
        for value in rows["secfilings"].dropna().astype(str):
            match = re.search(r"CIK=(\d+)", value)
            if match:
                return int(match.group(1))
    return None


def _companyfacts_json(cik: int) -> dict[str, Any]:
    padded = str(int(cik)).zfill(10)
    request = urllib.request.Request(
        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json",
        headers={
            "User-Agent": os.environ.get("SEC_USER_AGENT", "InvestorResearch/1.0 research-contact@example.com"),
            "Accept-Encoding": "gzip, deflate",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - official SEC public endpoint
        raw = response.read()
        if str(response.headers.get("Content-Encoding") or "").lower() == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        decoded = json.loads(raw.decode("utf-8"))
        return decoded if isinstance(decoded, dict) else {}


def _edgar_fact_checks(sf1: dict[str, Any], companyfacts: dict[str, Any]) -> list[dict[str, Any]]:
    period_end = str(sf1.get("calendardate") or "")[:10]
    filed = str(sf1.get("datekey") or "")[:10]
    dimension = str(sf1.get("dimension") or "").upper()
    checks: list[dict[str, Any]] = []
    for sf1_field, tags in {
        "revenue": ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues"),
        "assets": ("Assets",),
        "netinc": ("NetIncomeLoss", "ProfitLoss"),
    }.items():
        sf1_value = _float(sf1.get(sf1_field))
        if sf1_value is None:
            continue
        edgar_value = _extract_companyfact_value(
            companyfacts,
            tags,
            period_end=period_end,
            filed=filed,
            dimension=dimension,
            instant=sf1_field == "assets",
        )
        checks.append(
            {
                "field": sf1_field,
                "sf1_value": sf1_value,
                "edgar_value": edgar_value,
                "passed": edgar_value is not None and _within_tolerance(sf1_value, edgar_value),
            }
        )
    return checks


def _extract_companyfact_value(
    companyfacts: dict[str, Any],
    tags: Sequence[str],
    *,
    period_end: str,
    filed: str,
    dimension: str,
    instant: bool,
) -> float | None:
    facts = ((companyfacts.get("facts") or {}).get("us-gaap") or {})
    candidates: list[dict[str, Any]] = []
    period_ts = pd.Timestamp(period_end) if period_end else None
    for tag in tags:
        units = ((facts.get(tag) or {}).get("units") or {})
        for unit_name in ("USD", "shares", "USD/shares"):
            for item in units.get(unit_name) or []:
                end_text = str(item.get("end") or "")[:10]
                if period_ts is not None and end_text:
                    try:
                        if abs((pd.Timestamp(end_text) - period_ts).days) > 10:
                            continue
                    except Exception:
                        continue
                elif period_end:
                    continue
                if filed and str(item.get("filed") or "")[:10] > filed:
                    continue
                value = _float(item.get("val"))
                if value is not None:
                    start_text = str(item.get("start") or "")[:10]
                    duration = None
                    if start_text and end_text:
                        try:
                            duration = (pd.Timestamp(end_text) - pd.Timestamp(start_text)).days
                        except Exception:
                            duration = None
                    frame = str(item.get("frame") or "")
                    filed_text = str(item.get("filed") or "")[:10]
                    score = 0
                    score += 100 if filed_text == filed else 0
                    score += 20 if end_text == period_end else 0
                    if instant:
                        score += 30 if not start_text else 0
                        score += 10 if frame.endswith("I") else 0
                    elif dimension == "ARQ":
                        score += 35 if duration is not None and 45 <= duration <= 120 else 0
                        score += 15 if frame and not frame.endswith("I") and "Q" in frame else 0
                    elif dimension == "ARY":
                        score += 35 if duration is not None and 250 <= duration <= 380 else 0
                    elif dimension == "ART":
                        score += 20 if duration is not None and 250 <= duration <= 380 else 0
                    candidates.append({"filed": filed_text, "value": value, "score": score})
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item["score"], item["filed"]), reverse=True)
    return float(candidates[0]["value"])


def _within_tolerance(left: float, right: float, *, rel_tol: float = 0.02, abs_tol: float = 1_000.0) -> bool:
    return abs(left - right) <= max(abs_tol, abs(left) * rel_tol)


def edgar_validation_artifact_path(store_dir: str | Path = DEFAULT_SHARADAR_DIR) -> Path:
    return Path(store_dir) / "edgar_validation.json"


def load_edgar_validation_artifact(store_dir: str | Path = DEFAULT_SHARADAR_DIR) -> dict[str, Any]:
    path = edgar_validation_artifact_path(store_dir)
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    try:
        return dict(json.loads(path.read_text(encoding="utf-8")))
    except Exception as exc:
        return {"status": "INVALID", "path": str(path), "error": str(exc)}


def basket_study_gate_status(readiness: dict[str, Any], edgar_artifact: dict[str, Any], *, oos_start: str | None) -> str:
    base = certification_gate_status(str(readiness.get("data_readiness") or ""), after_tax=True, out_of_sample=bool(oos_start))
    if base != "certifiable":
        return base
    snapshot = str(readiness.get("data_snapshot_hash") or "")
    if str(edgar_artifact.get("status") or "").upper() == "PASS" and str(edgar_artifact.get("data_snapshot_hash") or "") == snapshot:
        return "certifiable"
    return "research_only_not_certifiable"


def basket_study_verdict(
    rows: list[dict[str, Any]],
    *,
    gate_status: str,
    readiness: dict[str, Any],
    edgar_artifact: dict[str, Any],
    arm_verdicts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_arm = {str(row.get("arm")): row for row in rows}
    index = by_arm.get("SPY_buy_hold") or {}
    static = by_arm.get("C0_static_basket") or by_arm.get("static_basket_equal_weight") or {}
    rules = [by_arm.get(arm) or {} for arm in ("A1_pure_momentum", "A2_quality_momentum", "A3_momentum_valuation_cap", "A4_quality_momentum_valuation")]
    best = max(rules, key=lambda row: _float(row.get("after_tax_terminal_wealth")) or -math.inf)
    best_beats_index = _beats(best, index)
    best_beats_static = _beats(best, static)
    no_rule_beats_index = not any(_beats(rule, index) for rule in rules)
    if no_rule_beats_index:
        status = "kill_switch_fail"
        recommendation = "No PIT selection rule beat the passive index after tax on risk-adjusted terms; recommend the passive index and stop basket-engine promotion."
    elif best_beats_static and best_beats_index:
        status = "win" if gate_status == "certifiable" else "not_certified"
        recommendation = "Best rule clears the pre-registered bar, but promotion is allowed only if the EDGAR/readiness gate is certifiable."
    elif best_beats_static:
        status = "inconclusive_not_certified"
        recommendation = "A rule beat the static basket but not the passive index; do not promote."
    else:
        status = "kill_switch_fail"
        recommendation = "No rule beat the static basket and index after tax; do not promote basket construction."
    if status == "win" and gate_status != "certifiable":
        status = "not_certified"
    return {
        "status": status,
        "best_rule": best.get("arm"),
        "best_index": index.get("arm"),
        "best_beats_static": bool(best_beats_static),
        "best_beats_index": bool(best_beats_index),
        "gate_status": gate_status,
        "data_readiness": readiness.get("data_readiness"),
        "edgar_validation_status": edgar_artifact.get("status"),
        "arm_verdicts": arm_verdicts or {},
        "recommended_next_step": recommendation,
        "production_default_changes": [],
    }


def basket_study_arm_verdicts(rows: list[dict[str, Any]], *, gate_status: str) -> dict[str, Any]:
    by_arm = {str(row.get("arm")): row for row in rows}
    static = by_arm.get("C0_static_basket") or by_arm.get("static_basket_equal_weight") or {}
    index = by_arm.get("SPY_buy_hold") or {}
    out: dict[str, Any] = {}
    for arm in SELECTION_ARMS:
        row = by_arm.get(arm) or {}
        if not row:
            continue
        beats_static = _beats_oos(row, static)
        beats_index = _beats_oos(row, index)
        if beats_static and beats_index:
            status = "win" if gate_status == "certifiable" else "inconclusive_not_certified"
        elif beats_static or beats_index:
            status = "inconclusive_not_certified"
        else:
            status = "kill_switch_fail"
        out[arm] = {
            "status": status,
            "beats_static_basket": bool(beats_static),
            "beats_synth_sp500": bool(beats_index),
            "gate_status": gate_status,
            "oos_total_return": _metric_value(row, "total_return", prefer_oos=True),
            "oos_calmar_ratio": _metric_value(row, "calmar_ratio", prefer_oos=True),
            "oos_ulcer_index": _metric_value(row, "ulcer_index", prefer_oos=True),
            "delta_vs_static": _relative_delta(row, static, prefer_oos=True),
            "delta_vs_synth_sp500": _relative_delta(row, index, prefer_oos=True),
        }
    return out


def basket_study_benchmark_relative_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_arm = {str(row.get("arm")): row for row in rows}
    benchmarks = {
        "synth_sp500": by_arm.get("SPY_buy_hold"),
        "static_basket": by_arm.get("C0_static_basket") or by_arm.get("static_basket_equal_weight"),
    }
    output: list[dict[str, Any]] = []
    for row in rows:
        arm = str(row.get("arm"))
        for label, benchmark in benchmarks.items():
            if not benchmark or arm == str(benchmark.get("arm")):
                continue
            output.append(
                {
                    "arm": arm,
                    "benchmark": label,
                    "after_tax_terminal_wealth_delta": _delta(row.get("after_tax_terminal_wealth"), benchmark.get("after_tax_terminal_wealth")),
                    "total_return_delta": _delta(row.get("total_return"), benchmark.get("total_return")),
                    "cagr_delta": _delta(row.get("annualized_return"), benchmark.get("annualized_return")),
                    "calmar_delta": _delta(row.get("calmar_ratio"), benchmark.get("calmar_ratio")),
                    "ulcer_delta": _delta(row.get("ulcer_index"), benchmark.get("ulcer_index")),
                    "oos_total_return_delta": _delta(_metric_value(row, "total_return", prefer_oos=True), _metric_value(benchmark, "total_return", prefer_oos=True)),
                    "oos_calmar_delta": _delta(_metric_value(row, "calmar_ratio", prefer_oos=True), _metric_value(benchmark, "calmar_ratio", prefer_oos=True)),
                    "oos_ulcer_delta": _delta(_metric_value(row, "ulcer_index", prefer_oos=True), _metric_value(benchmark, "ulcer_index", prefer_oos=True)),
                }
            )
    return output


def survivorship_bias_delta(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_arm = {str(row.get("arm")): row for row in rows}
    static = by_arm.get("C0_static_basket") or {}
    pit = by_arm.get("C0b_static_pit") or {}
    static_wealth = _float(static.get("after_tax_terminal_wealth"), 0.0) or 0.0
    pit_wealth = _float(pit.get("after_tax_terminal_wealth"), 0.0) or 0.0
    static_return = _float(static.get("total_return"), 0.0) or 0.0
    pit_return = _float(pit.get("total_return"), 0.0) or 0.0
    static_cagr = _float(static.get("annualized_return"), 0.0) or 0.0
    pit_cagr = _float(pit.get("annualized_return"), 0.0) or 0.0
    return {
        "definition": "C0_static_basket minus C0b_static_pit",
        "after_tax_terminal_wealth_delta": static_wealth - pit_wealth,
        "total_return_delta": static_return - pit_return,
        "cagr_delta": static_cagr - pit_cagr,
    }


def basket_study_terminal_event_use_counts(results: dict[str, dict[str, Any]], store: SharadarStore) -> list[dict[str, Any]]:
    counts: dict[int, dict[str, Any]] = {}
    universe: set[int] = set()
    for arm, payload in results.items():
        for rows in (payload.get("selection_history") or {}).values():
            for row in rows:
                if isinstance(row, dict) and row.get("permaticker") is not None:
                    universe.add(int(row["permaticker"]))
        for event in payload.get("terminal_events") or []:
            if isinstance(event, dict) and event.get("permaticker") is not None:
                universe.add(int(event["permaticker"]))
        for trade in payload.get("trades") or []:
            if not isinstance(trade, dict) or str(trade.get("exit_type") or "") != "terminal_value":
                continue
            perma = _security_to_perma(str(trade.get("ticker") or "0"))
            if perma <= 0:
                continue
            universe.add(perma)
            row = counts.setdefault(
                perma,
                {
                    "permaticker": perma,
                    "ticker": store.ticker_for_permaticker(perma),
                    "use_count": 0,
                    "quantity": 0.0,
                    "arms": set(),
                    "terminal_value": float(trade.get("price") or 0.0),
                    "terminal_value_source": str(trade.get("terminal_value_source") or ""),
                    "terminal_value_reason": str(trade.get("terminal_value_reason") or ""),
                    "requires_human_review": bool(trade.get("requires_human_review", False)),
                },
            )
            row["use_count"] = int(row["use_count"]) + 1
            row["quantity"] = float(row["quantity"]) + float(trade.get("quantity") or 0.0)
            row["arms"].add(str(arm))
            row["terminal_value"] = float(trade.get("price") or 0.0)
            row["terminal_value_source"] = str(trade.get("terminal_value_source") or row.get("terminal_value_source") or "")
            row["terminal_value_reason"] = str(trade.get("terminal_value_reason") or row.get("terminal_value_reason") or "")
            row["requires_human_review"] = bool(trade.get("requires_human_review") or row.get("requires_human_review"))
    for perma, event in store.terminal_value_events(sorted(universe)).items():
        row = counts.setdefault(
            int(perma),
            {
                "permaticker": int(perma),
                "ticker": store.ticker_for_permaticker(int(perma), as_of_date=event.date),
                "use_count": 0,
                "quantity": 0.0,
                "arms": set(),
            },
        )
        row["terminal_value"] = float(event.value)
        row["terminal_value_source"] = str(event.source)
        row["terminal_value_reason"] = str(event.reason)
        row["requires_human_review"] = bool(event.requires_human_review)
    out: list[dict[str, Any]] = []
    for row in counts.values():
        copied = dict(row)
        copied["arms"] = sorted(copied["arms"])
        copied["held"] = int(copied.get("use_count") or 0) > 0
        out.append(copied)
    return sorted(out, key=lambda item: (str(item.get("ticker") or ""), int(item.get("permaticker") or 0)))


def terminal_value_disclosure_summary(terminal_event_use_counts: Sequence[dict[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in terminal_event_use_counts]
    conservative = [
        {
            "ticker": row.get("ticker"),
            "permaticker": row.get("permaticker"),
            "reason": row.get("terminal_value_reason"),
            "use_count": int(row.get("use_count") or 0),
            "held": bool(row.get("held") or int(row.get("use_count") or 0) > 0),
            "requires_human_review": bool(row.get("requires_human_review")),
        }
        for row in rows
        if str(row.get("terminal_value_source") or "") == "conservative_default_zero"
    ]
    unknown_residuals = [
        {
            "ticker": row.get("ticker"),
            "permaticker": row.get("permaticker"),
            "source": row.get("terminal_value_source"),
            "terminal_value": row.get("terminal_value"),
            "reason": row.get("terminal_value_reason"),
            "use_count": int(row.get("use_count") or 0),
            "held": bool(row.get("held") or int(row.get("use_count") or 0) > 0),
        }
        for row in rows
        if str(row.get("terminal_value_source") or "") in TERMINAL_UNKNOWN_RESIDUAL_SOURCES
    ]
    review = [
        {
            "ticker": row.get("ticker"),
            "permaticker": row.get("permaticker"),
            "reason": row.get("terminal_value_reason"),
            "source": row.get("terminal_value_source"),
            "use_count": int(row.get("use_count") or 0),
            "held": bool(row.get("held") or int(row.get("use_count") or 0) > 0),
        }
        for row in rows
        if bool(row.get("held") or int(row.get("use_count") or 0) > 0)
        and (
            str(row.get("terminal_value_source") or "") in TERMINAL_UNKNOWN_RESIDUAL_SOURCES
            or str(row.get("terminal_value_source") or "") == "acquisition_missing_price_zero"
            or str(row.get("terminal_value_source") or "") == "conservative_default_zero"
        )
    ]
    breakdown: dict[str, int] = {}
    held_by_source: dict[str, int] = {}
    for row in rows:
        source = str(row.get("terminal_value_source") or "unknown")
        breakdown[source] = int(breakdown.get(source, 0)) + 1
        if bool(row.get("held") or int(row.get("use_count") or 0) > 0):
            held_by_source[source] = int(held_by_source.get(source, 0)) + 1
    last_price_count = sum(int(breakdown.get(source, 0)) for source in TERMINAL_LAST_PRICE_SOURCES)
    failure_zero_count = sum(int(breakdown.get(source, 0)) for source in TERMINAL_FAILURE_ZERO_SOURCES)
    held_last_price_count = sum(int(held_by_source.get(source, 0)) for source in TERMINAL_LAST_PRICE_SOURCES)
    return {
        "policy": "Reason-dependent terminal values: failures/receiverships/liquidations mark to $0; acquisition-like delistings mark to the last traded adjusted SEP price; unknown residuals branch by pre-delisting health and are disclosed.",
        "category_breakdown": dict(sorted(breakdown.items())),
        "held_category_breakdown": dict(sorted(held_by_source.items())),
        "failure_zero_count": failure_zero_count,
        "last_price_terminal_count": last_price_count,
        "held_last_price_terminal_count": held_last_price_count,
        "unknown_residuals": sorted(unknown_residuals, key=lambda row: (str(row.get("ticker") or ""), int(row.get("permaticker") or 0))),
        "unknown_residual_count": len(unknown_residuals),
        "conservative_defaults": sorted(conservative, key=lambda row: (str(row.get("ticker") or ""), int(row.get("permaticker") or 0))),
        "conservative_default_count": len(conservative),
        "held_name_review_list": sorted(review, key=lambda row: (str(row.get("ticker") or ""), int(row.get("permaticker") or 0))),
        "held_name_review_count": len(review),
        "held_terminal_changed_results_vs_prior_run": held_last_price_count > 0 or any(bool(row.get("held")) for row in conservative),
        "production_defaults_changed": False,
    }


def terminal_value_policy_comparison(
    previous_summary: dict[str, Any] | None,
    current_rows: Sequence[dict[str, Any]],
    terminal_event_use_counts: Sequence[dict[str, Any]],
    *,
    current_snapshot_hash: str | None,
) -> dict[str, Any]:
    previous = dict(previous_summary or {})
    previous_rows = {str(row.get("arm") or ""): dict(row) for row in previous.get("rows") or [] if isinstance(row, dict)}
    current_by_arm = {str(row.get("arm") or ""): dict(row) for row in current_rows if isinstance(row, dict)}
    comparison_rows: list[dict[str, Any]] = []
    for arm in TERMINAL_POLICY_COMPARISON_ARMS:
        before = previous_rows.get(arm, {})
        after = current_by_arm.get(arm, {})
        comparison_rows.append(
            {
                "arm": arm,
                "before_after_tax_terminal_wealth": before.get("after_tax_terminal_wealth"),
                "after_after_tax_terminal_wealth": after.get("after_tax_terminal_wealth"),
                "after_tax_terminal_wealth_delta": _delta(after.get("after_tax_terminal_wealth"), before.get("after_tax_terminal_wealth")),
                "before_cagr": before.get("annualized_return"),
                "after_cagr": after.get("annualized_return"),
                "cagr_delta": _delta(after.get("annualized_return"), before.get("annualized_return")),
                "before_max_drawdown": before.get("max_drawdown"),
                "after_max_drawdown": after.get("max_drawdown"),
                "before_source_snapshot": previous.get("data_snapshot_hash"),
                "after_source_snapshot": current_snapshot_hash,
            }
        )
    previous_terminal_by_perma: dict[int, dict[str, Any]] = {}
    for row in previous.get("terminal_event_use_counts") or []:
        if not isinstance(row, dict):
            continue
        perma = _parse_int(row.get("permaticker"))
        if perma is not None:
            previous_terminal_by_perma[int(perma)] = dict(row)
    flipped: list[dict[str, Any]] = []
    for row in terminal_event_use_counts:
        source = str(row.get("terminal_value_source") or "")
        perma = _parse_int(row.get("permaticker"))
        previous_terminal = previous_terminal_by_perma.get(int(perma)) if perma is not None else None
        previous_source = str((previous_terminal or {}).get("terminal_value_source") or "")
        if bool(row.get("held") or int(row.get("use_count") or 0) > 0) and source in TERMINAL_LAST_PRICE_SOURCES and previous_source in {"conservative_default_zero", "unknown_distressed_zero", ""}:
            flipped.append(
                {
                    "ticker": row.get("ticker"),
                    "permaticker": perma,
                    "source": source,
                    "previous_source": previous_source or "missing_from_prior_summary",
                    "terminal_value": row.get("terminal_value"),
                    "use_count": int(row.get("use_count") or 0),
                    "arms": list(row.get("arms") or []),
                }
            )
    return {
        "policy": "Before is the prior terminal-policy summary in the campaign directory; after is the current reason-dependent run.",
        "baseline_snapshot_hash": previous.get("data_snapshot_hash"),
        "current_snapshot_hash": current_snapshot_hash,
        "comparison_rows": comparison_rows,
        "held_flipped_to_last_price_count": len(flipped),
        "held_flipped_to_last_price": sorted(flipped, key=lambda row: (str(row.get("ticker") or ""), int(row.get("permaticker") or 0))),
        "production_defaults_changed": False,
    }


def render_basket_study_report(
    *,
    campaign_dir: str | Path = DEFAULT_BASKET_STUDY_DIR,
    output_dir: str | Path = DEFAULT_BASKET_STUDY_REPORT_DIR,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(campaign_dir)
    summary = dict(_read_json(root / "summary.json"))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(output_path) if output_path is not None else out_dir / "management_report.html"
    report_path.write_text(_basket_report_html(summary), encoding="utf-8")
    return report_path


def basket_study_status(campaign_dir: str | Path = DEFAULT_BASKET_STUDY_DIR) -> dict[str, Any]:
    root = Path(campaign_dir)
    return {
        "campaign_dir": str(root),
        "summary_exists": (root / "summary.json").exists(),
        "result_count": len(list((root / "results").glob("*.json"))) if (root / "results").exists() else 0,
    }


def _candidate_features(store: SharadarStore, as_of: pd.Timestamp, cfg: BasketStudyConfig) -> list[SelectionRow]:
    cache_key = (
        str(store.root.resolve()),
        pd.Timestamp(as_of).date().isoformat(),
        cfg.formation,
        cfg.min_dollar_adv,
        cfg.min_marketcap,
        cfg.min_listing_days,
        cfg.dollar_adv_days,
        cfg.universe_top_n,
    )
    cached = _CANDIDATE_FEATURE_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)
    universe = store.universe_asof(as_of, top_n=cfg.universe_top_n)
    universe = _filter_common_stock_universe(store, universe, as_of, cfg)
    if not universe:
        return []
    start = (as_of - pd.DateOffset(months=14 if cfg.formation == "12_1" else 8)).date().isoformat()
    end = as_of.date().isoformat()
    price_frames = store.get_prices(universe, start, end)
    daily = _daily_snapshot(store, universe, as_of)
    fundamentals_by_perma = _fundamentals_snapshot(store, universe, as_of)
    ticker_labels = _ticker_labels_for_permatickers(store, universe, as_of)
    rows: list[SelectionRow] = []
    for perma in universe:
        frame = price_frames.get(int(perma), pd.DataFrame())
        if frame.empty:
            continue
        frame = _normalize_frame(frame)
        if frame.empty:
            continue
        listing_days = (as_of - frame.index.min()).days
        if listing_days < cfg.min_listing_days:
            continue
        adv = _dollar_adv(frame, days=cfg.dollar_adv_days)
        if adv is None or adv < cfg.min_dollar_adv:
            continue
        market = daily.get(int(perma), {})
        marketcap = _float(market.get("marketcap"))
        if marketcap is not None and not _passes_marketcap_floor(marketcap, cfg.min_marketcap):
            continue
        fundamentals = fundamentals_by_perma.get(int(perma), {})
        quality = _quality_factor(fundamentals)
        valuation = _valuation_factor(fundamentals, market)
        rows.append(
            SelectionRow(
                permaticker=int(perma),
                ticker=ticker_labels.get(int(perma), str(perma)),
                score=0.0,
                momentum=_momentum(frame, as_of, cfg.formation),
                quality=quality,
                valuation=valuation,
                marketcap=marketcap,
                dollar_adv=adv,
            )
        )
    _CANDIDATE_FEATURE_CACHE[cache_key] = tuple(rows)
    return rows


def _filter_common_stock_universe(store: SharadarStore, permatickers: Sequence[int], as_of: pd.Timestamp, cfg: BasketStudyConfig) -> list[int]:
    tickers = store.read_meta_table("TICKERS")
    if tickers.empty or "permaticker" not in tickers.columns:
        return [int(item) for item in permatickers]
    rows = tickers.copy()
    rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
    rows = rows.loc[rows["permaticker"].isin([int(item) for item in permatickers])].copy()
    if "table" in rows.columns:
        rows = rows.loc[rows["table"].astype(str).str.upper().isin({"SEP", "SF1"})].copy()
    if "category" in rows.columns:
        rows = rows.loc[rows["category"].astype(str).str.contains("Common Stock", case=False, na=False)].copy()
    if "currency" in rows.columns:
        rows = rows.loc[rows["currency"].astype(str).str.upper().eq("USD")].copy()
    if "location" in rows.columns:
        rows = rows.loc[rows["location"].astype(str).str.contains("U.S.A|United States", case=False, regex=True, na=False)].copy()
    rows["_first"] = pd.to_datetime(rows.get("firstpricedate"), errors="coerce") if "firstpricedate" in rows.columns else pd.NaT
    rows["_last"] = pd.to_datetime(rows.get("lastpricedate"), errors="coerce") if "lastpricedate" in rows.columns else pd.NaT
    rows = rows.loc[
        (rows["_first"].isna() | (rows["_first"] <= as_of - pd.Timedelta(days=cfg.min_listing_days)))
        & (rows["_last"].isna() | (rows["_last"] >= as_of))
    ]
    return sorted(pd.to_numeric(rows["permaticker"], errors="coerce").dropna().astype(int).unique().tolist())


def _ticker_labels_for_permatickers(store: SharadarStore, permatickers: Sequence[int], as_of: pd.Timestamp) -> dict[int, str]:
    tickers = store.read_meta_table("TICKERS")
    wanted = sorted({int(item) for item in permatickers})
    if tickers.empty or "permaticker" not in tickers.columns or not wanted:
        return {perma: str(perma) for perma in wanted}
    rows = tickers.copy()
    rows["permaticker"] = pd.to_numeric(rows["permaticker"], errors="coerce")
    rows = rows.loc[rows["permaticker"].isin(wanted)].copy()
    if rows.empty:
        return {perma: str(perma) for perma in wanted}
    rows["_start"] = pd.to_datetime(rows.get("firstpricedate"), errors="coerce") if "firstpricedate" in rows.columns else pd.NaT
    rows["_end"] = pd.to_datetime(rows.get("lastpricedate"), errors="coerce") if "lastpricedate" in rows.columns else pd.NaT
    active = rows.loc[
        (rows["_start"].isna() | (rows["_start"] <= as_of))
        & (rows["_end"].isna() | (rows["_end"] >= as_of))
    ].copy()
    if not active.empty:
        rows = active
    ticker_column = "ticker" if "ticker" in rows.columns else "symbol" if "symbol" in rows.columns else None
    if ticker_column is None:
        return {perma: str(perma) for perma in wanted}
    rows["_sort_start"] = rows["_start"].fillna(pd.Timestamp.min)
    rows = rows.sort_values(["permaticker", "_sort_start"])
    labels: dict[int, str] = {}
    for perma, group in rows.groupby("permaticker"):
        labels[int(perma)] = str(group.iloc[-1].get(ticker_column) or int(perma)).upper()
    return {perma: labels.get(perma, str(perma)) for perma in wanted}


def _static_basket_rows(store: SharadarStore, as_of: pd.Timestamp, cfg: BasketStudyConfig, basket: dict[str, Any]) -> list[SelectionRow]:
    tickers = [str(item).upper() for item in basket.get("tickers") or [] if str(item).strip()]
    rows: list[SelectionRow] = []
    for idx, ticker in enumerate(tickers):
        resolution = store.resolve_ticker(ticker, as_of_date=as_of)
        if resolution is None:
            continue
        rows.append(SelectionRow(resolution.permaticker, ticker, float(len(tickers) - idx), None, None, None, None, None))
    return rows[: cfg.basket_size]


def _score_selection_rows(rows: list[SelectionRow], arm: str, cfg: BasketStudyConfig) -> list[SelectionRow]:
    valuation_cutoff = None
    valid_valuations = [row.valuation for row in rows if row.valuation is not None]
    if valid_valuations:
        valuation_cutoff = pd.Series(valid_valuations).quantile(cfg.expensive_decile)
    mom_z = _zmap({row.permaticker: row.momentum for row in rows})
    qual_z = _zmap({row.permaticker: row.quality for row in rows})
    scored: list[SelectionRow] = []
    for row in rows:
        if arm in {"A3_momentum_valuation_cap", "A4_quality_momentum_valuation"} and valuation_cutoff is not None and row.valuation is not None and row.valuation >= valuation_cutoff:
            continue
        if arm == "A1_pure_momentum":
            score = mom_z.get(row.permaticker, -999.0)
        elif arm == "A2_quality_momentum":
            score = mom_z.get(row.permaticker, -999.0) + qual_z.get(row.permaticker, -999.0)
        elif arm == "A3_momentum_valuation_cap":
            score = mom_z.get(row.permaticker, -999.0)
        elif arm == "A4_quality_momentum_valuation":
            score = mom_z.get(row.permaticker, -999.0) + qual_z.get(row.permaticker, -999.0)
        else:
            score = 0.0
        scored.append(SelectionRow(row.permaticker, row.ticker, float(score), row.momentum, row.quality, row.valuation, row.marketcap, row.dollar_adv))
    return scored


def _daily_snapshot(store: SharadarStore, permatickers: Sequence[int], as_of: pd.Timestamp) -> dict[int, dict[str, Any]]:
    daily = store.read_fact_table(
        "DAILY",
        columns=["permaticker", "date", "marketcap", "ev", "evebitda", "pe", "pb"],
        filters=[("permaticker", "in", [int(item) for item in permatickers])],
    )
    if daily.empty:
        return {}
    daily["permaticker"] = pd.to_numeric(daily["permaticker"], errors="coerce").astype("Int64")
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
    rows = daily.loc[daily["date"] <= as_of].copy()
    if rows.empty:
        return {}
    rows = rows.sort_values(["permaticker", "date"]).groupby("permaticker", as_index=False).tail(1)
    return {int(row["permaticker"]): dict(row) for _, row in rows.dropna(subset=["permaticker"]).iterrows()}


def _fundamentals_snapshot(store: SharadarStore, permatickers: Sequence[int], as_of: pd.Timestamp) -> dict[int, dict[str, Any]]:
    wanted = sorted({int(item) for item in permatickers})
    if not wanted:
        return {}
    columns = ["permaticker", "datekey", "dimension", *QUALITY_FIELDS]
    sf1 = store.read_fact_table(
        "SF1",
        columns=list(dict.fromkeys(columns)),
        filters=[
            ("permaticker", "in", wanted),
            ("datekey", "<=", pd.Timestamp(as_of).date().isoformat()),
        ],
    )
    if sf1.empty or "permaticker" not in sf1.columns or "datekey" not in sf1.columns:
        return {}
    sf1 = sf1.copy()
    sf1["permaticker"] = pd.to_numeric(sf1["permaticker"], errors="coerce").astype("Int64")
    sf1["datekey"] = pd.to_datetime(sf1["datekey"], errors="coerce")
    rows = sf1.loc[sf1["permaticker"].isin(wanted) & (sf1["datekey"] <= pd.Timestamp(as_of))].copy()
    if rows.empty:
        return {}
    if "dimension" in rows.columns:
        dim_order = {name: idx for idx, name in enumerate(("ARQ", "ART", "ARY"))}
        rows = rows.loc[rows["dimension"].astype(str).str.upper().isin(dim_order)].copy()
        rows["_dimension_order"] = rows["dimension"].astype(str).str.upper().map(dim_order).fillna(999)
    else:
        rows["_dimension_order"] = 999
        rows["dimension"] = ""
    rows = rows.sort_values(["permaticker", "datekey", "_dimension_order"], ascending=[True, False, True])
    latest = rows.groupby("permaticker", as_index=False).head(1)
    out: dict[int, dict[str, Any]] = {}
    for _, row in latest.dropna(subset=["permaticker"]).iterrows():
        payload = {
            "permaticker": int(row["permaticker"]),
            "datekey": pd.Timestamp(row["datekey"]).date().isoformat() if pd.notna(row["datekey"]) else None,
            "dimension": str(row.get("dimension") or ""),
        }
        for field in QUALITY_FIELDS:
            payload[field] = row.get(field)
        out[int(row["permaticker"])] = payload
    return out


def _momentum(frame: pd.DataFrame, as_of: pd.Timestamp, formation: str) -> float | None:
    months = 12 if formation == "12_1" else 6
    end_date = as_of - pd.DateOffset(months=1)
    start_date = as_of - pd.DateOffset(months=months + 1)
    price = frame["price"].astype(float).sort_index()
    start_px = _last_price_on_or_before(price, start_date)
    end_px = _last_price_on_or_before(price, end_date)
    if start_px is None or end_px is None or start_px <= 0:
        return None
    return float(end_px / start_px - 1.0)


def _quality_factor(fields: dict[str, Any]) -> float | None:
    revenue = _float(fields.get("revenue"))
    assets = _float(fields.get("assets"))
    liabilities = _float(fields.get("liabilities"))
    ebit = _float(fields.get("ebit"))
    taxexp = _float(fields.get("taxexp"), 0.0)
    debt = _float(fields.get("debt"), 0.0)
    equity = _float(fields.get("equity"), 0.0)
    fcf = _float(fields.get("fcf"))
    if fcf is None:
        ncfo = _float(fields.get("ncfo"))
        capex = _float(fields.get("capex"))
        fcf = ncfo - abs(capex) if ncfo is not None and capex is not None else None
    gp = _float(fields.get("gp"))
    values: list[float] = []
    invested = (debt or 0.0) + (equity or 0.0)
    if ebit is not None and invested > 0:
        values.append((ebit - (taxexp or 0.0)) / invested)
    if fcf is not None and revenue and revenue > 0:
        values.append(fcf / revenue)
    if gp is not None and assets and assets > 0:
        values.append(gp / assets)
    if liabilities is not None and assets and assets > 0:
        values.append(-(liabilities / assets))
    if not values:
        return None
    return float(sum(values) / len(values))


def _valuation_factor(fields: dict[str, Any], market: dict[str, Any]) -> float | None:
    marketcap = _float(market.get("marketcap"))
    ev = _float(market.get("ev"))
    ebitda = _float(fields.get("ebitda"))
    netinc = _float(fields.get("netinc"))
    fcf = _float(fields.get("fcf"))
    pe = _float(market.get("pe"))
    evebitda = _float(market.get("evebitda"))
    values: list[float] = []
    if evebitda is not None and evebitda > 0:
        values.append(evebitda)
    elif ev is not None and ebitda is not None and ebitda > 0:
        values.append(ev / ebitda)
    if marketcap is not None and fcf is not None and fcf > 0:
        values.append(marketcap / fcf)
    if pe is not None and pe > 0:
        values.append(pe)
    elif marketcap is not None and netinc is not None and netinc > 0:
        values.append(marketcap / netinc)
    if not values:
        return None
    return float(sum(values) / len(values))


def _execute_terminal_value_events(
    *,
    date: pd.Timestamp,
    lots: list[CCELLot],
    cash: float,
    terminal_events: dict[int, Any],
    processed: set[int],
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]]]:
    trades: list[dict[str, Any]] = []
    realized: list[dict[str, Any]] = []
    for perma, event in sorted(terminal_events.items()):
        if perma in processed or pd.Timestamp(event.date).normalize() > pd.Timestamp(date).normalize():
            continue
        sec = _perma_security(int(perma))
        qty = sum(lot.quantity for lot in lots if lot.ticker == sec)
        processed.add(int(perma))
        if qty <= 0:
            continue
        price = float(event.value)
        notional = qty * price
        realized.extend(_sell_fifo_lots(lots, sec, qty, price, _date_text(date), "terminal_value"))
        cash += notional
        trade = _trade(date, sec, "Sell", qty, price, notional, 0.0, "terminal_value")
        trade["terminal_value_source"] = str(event.source)
        trade["terminal_value_reason"] = str(event.reason)
        trade["requires_human_review"] = bool(getattr(event, "requires_human_review", False))
        trades.append(trade)
    return cash, trades, realized


def _rebalance_to_targets(
    *,
    date: pd.Timestamp,
    target_holdings: set[int],
    lots: list[CCELLot],
    cash: float,
    open_prices: dict[int, float],
    cfg: BasketStudyConfig,
    next_lot_id: int,
    rebalance_weights: bool,
) -> tuple[float, int, list[dict[str, Any]], list[dict[str, Any]], float, float]:
    trades: list[dict[str, Any]] = []
    realized: list[dict[str, Any]] = []
    costs = 0.0
    turnover = 0.0
    held = {_security_to_perma(lot.ticker) for lot in lots}
    to_sell = held - target_holdings
    for perma in sorted(to_sell):
        sec = _perma_security(perma)
        qty = sum(lot.quantity for lot in lots if lot.ticker == sec)
        price = float(open_prices.get(perma, 0.0))
        if qty <= 0 or price <= 0:
            continue
        cost = qty * price * cfg.exit_cost_bps / 10_000.0
        proceeds_per_share = price - cost / qty
        realized.extend(_sell_fifo_lots(lots, sec, qty, proceeds_per_share, _date_text(date), "annual_reconstitution_drop"))
        notional = qty * price
        cash += notional - cost
        costs += cost
        turnover += notional / cfg.starting_cash
        trades.append(_trade(date, sec, "Sell", qty, price, notional, cost, "annual_reconstitution_drop"))

    current_values = {
        _security_to_perma(lot.ticker): sum(item.quantity for item in lots if item.ticker == lot.ticker) * float(open_prices.get(_security_to_perma(lot.ticker), 0.0))
        for lot in lots
    }
    equity = cash + sum(current_values.values())
    new_targets = sorted(target_holdings - {_security_to_perma(lot.ticker) for lot in lots})
    if rebalance_weights:
        for perma in sorted(target_holdings):
            sec = _perma_security(perma)
            price = float(open_prices.get(perma, 0.0))
            if price <= 0:
                continue
            target_value = equity / max(1, len(target_holdings))
            current_value = current_values.get(perma, 0.0)
            delta = target_value - current_value
            if delta < -price:
                qty = min(sum(lot.quantity for lot in lots if lot.ticker == sec), abs(delta) / price)
                cost = qty * price * cfg.exit_cost_bps / 10_000.0
                realized.extend(_sell_fifo_lots(lots, sec, qty, price - cost / qty, _date_text(date), "equal_weight_rebalance"))
                notional = qty * price
                cash += notional - cost
                costs += cost
                turnover += notional / cfg.starting_cash
                trades.append(_trade(date, sec, "Sell", qty, price, notional, cost, "equal_weight_rebalance"))
            elif delta > price:
                cash, next_lot_id, trade, cost, turn = _buy_lot(date, sec, price, min(delta, cash), cash, next_lot_id, cfg, "equal_weight_rebalance")
                if trade:
                    _append_new_lot(lots, trade)
                    trades.append(trade)
                    costs += cost
                    turnover += turn
        return cash, next_lot_id, trades, realized, costs, turnover

    deploy_each = cash / max(1, len(new_targets)) if new_targets else 0.0
    for perma in new_targets:
        price = float(open_prices.get(perma, 0.0))
        if price <= 0 or deploy_each <= 0:
            continue
        cash, next_lot_id, trade, cost, turn = _buy_lot(date, _perma_security(perma), price, min(deploy_each, cash), cash, next_lot_id, cfg, "annual_reconstitution_buy")
        if trade:
            _append_new_lot(lots, trade)
            trades.append(trade)
            costs += cost
            turnover += turn
    return cash, next_lot_id, trades, realized, costs, turnover


def _buy_lot(
    date: pd.Timestamp,
    security: str,
    price: float,
    gross_cash: float,
    cash: float,
    next_lot_id: int,
    cfg: BasketStudyConfig,
    reason: str,
) -> tuple[float, int, dict[str, Any] | None, float, float]:
    unit_cost = price * (1.0 + cfg.entry_cost_bps / 10_000.0)
    qty = math.floor(gross_cash / unit_cost)
    if qty <= 0:
        return cash, next_lot_id, None, 0.0, 0.0
    notional = qty * price
    cost = notional * cfg.entry_cost_bps / 10_000.0
    if notional + cost > cash + 1e-6:
        return cash, next_lot_id, None, 0.0, 0.0
    cash -= notional + cost
    trade = _trade(date, security, "Buy", qty, price, notional, cost, reason)
    # The caller owns the lot list, so use a side channel on the trade for reconstruction.
    trade["_new_lot"] = CCELLot(next_lot_id, security, qty, price + cost / qty, _date_text(date)).to_dict()
    return cash, next_lot_id + 1, trade, cost, notional / cfg.starting_cash


def _trade(date: pd.Timestamp, ticker: str, side: str, qty: float, price: float, notional: float, cost: float, reason: str) -> dict[str, Any]:
    return {
        "date": _date_text(date),
        "ticker": ticker,
        "side": side,
        "quantity": qty,
        "price": price,
        "notional": notional,
        "costs_paid": cost,
        "net_pnl": -cost,
        "exit_type": reason,
    }


def _segment_metrics(equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]], benchmark_curve: pd.DataFrame | None, start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, Any]:
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
    segment_trades = [row for row in trades if (start is None or pd.Timestamp(row["date"]) >= start) and (end is None or pd.Timestamp(row["date"]) < end)]
    return _metrics(segment.to_dict("records"), segment_trades, benchmark_curve=benchmark_curve)


def run_vol_target_benchmark(frame: pd.DataFrame, cfg: BasketStudyConfig, *, benchmark_curve: pd.DataFrame, windows: list[StressWindow]) -> dict[str, Any]:
    normalized = _normalize_frame(frame)
    returns = normalized["price"].pct_change().fillna(0.0)
    realized_vol = returns.rolling(63).std().fillna(returns.expanding().std()).fillna(0.0) * math.sqrt(252)
    target_vol = 0.15
    exposure = (target_vol / realized_vol.replace(0, pd.NA)).clip(lower=0.0, upper=1.0).fillna(1.0)
    equity = cfg.starting_cash
    curve: list[dict[str, Any]] = []
    for idx, (date, ret) in enumerate(returns.items()):
        if idx > 0:
            equity *= 1.0 + float(exposure.loc[date]) * float(ret)
        curve.append({"date": _date_text(date), "equity": equity, "cash": 0.0, "position_value": equity * float(exposure.loc[date]), "exposure": float(exposure.loc[date]), "costs_paid": 0.0, "turnover": 0.0})
    trades: list[dict[str, Any]] = []
    metrics = _metrics(curve, trades, benchmark_curve=benchmark_curve)
    metrics["after_tax_terminal_wealth"] = curve[-1]["equity"] if curve else None
    metrics["pre_tax_terminal_wealth"] = curve[-1]["equity"] if curve else None
    metrics["annualized_turnover"] = 0.0
    metrics["total_costs_paid"] = 0.0
    return {
        "schema": "regime_basket_study_benchmark.v1",
        "arm": "L1_vol_target",
        "config": cfg.to_dict(),
        "metrics": _json_safe(metrics),
        "pre_tax_metrics": _json_safe(metrics),
        "equity_curve": _json_safe(curve),
        "after_tax_equity_curve": _json_safe(curve),
        "trades": trades,
        "stress_windows": _json_safe(_stress_results_for_curve(pd.DataFrame(curve), trades, benchmark_curve, windows)),
        "production_defaults_changed": False,
    }


def _stamp_benchmark_payload(payload: dict[str, Any], cfg: BasketStudyConfig, arm: str, *, benchmark_source: str | None = None) -> dict[str, Any]:
    copied = dict(payload)
    copied["arm"] = arm
    copied["config"] = cfg.to_dict()
    copied["production_defaults_changed"] = False
    if benchmark_source:
        copied["benchmark_source"] = benchmark_source
        copied.setdefault("metrics", {})
        if isinstance(copied["metrics"], dict):
            copied["metrics"]["benchmark_source"] = benchmark_source
    return copied


def _buy_hold_curve(frame: pd.DataFrame, *, starting_cash: float) -> pd.DataFrame:
    normalized = _normalize_frame(frame)
    if normalized.empty:
        return pd.DataFrame()
    first = float(normalized["price"].iloc[0])
    shares = starting_cash / first if first > 0 else 0.0
    return pd.DataFrame({"date": [_date_text(idx) for idx in normalized.index], "equity": shares * normalized["price"].astype(float).to_numpy()})


def _panel_dates_by_permaticker(frames: dict[int, pd.DataFrame]) -> list[pd.Timestamp]:
    dates: set[pd.Timestamp] = set()
    for frame in frames.values():
        dates.update(pd.Timestamp(index).normalize() for index in frame.index)
    return sorted(dates)


def _matching_selection_date(selections: dict[str, list[SelectionRow]], date: pd.Timestamp, executed: set[str]) -> str | None:
    due = [key for key in selections if key not in executed and pd.Timestamp(key) <= pd.Timestamp(date)]
    return sorted(due)[0] if due else None


def _append_new_lot(lots: list[CCELLot], trade: dict[str, Any]) -> None:
    payload = trade.pop("_new_lot", None)
    if not isinstance(payload, dict):
        return
    lots.append(
        CCELLot(
            lot_id=int(payload["lot_id"]),
            ticker=str(payload["ticker"]),
            quantity=float(payload["quantity"]),
            basis_per_share=float(payload["basis_per_share"]),
            acquisition_date=str(payload["acquisition_date"]),
        )
    )


def _perma_security(perma: int) -> str:
    return f"P{int(perma)}"


def _security_to_perma(security: str) -> int:
    text = str(security)
    return int(text[1:] if text.startswith("P") else text)


def _mark_price_for_security(last_prices: dict[int, float], security: str) -> float | None:
    perma = _security_to_perma(security)
    value = last_prices.get(perma)
    return float(value) if value is not None else None


def _mark_prices_for_lots(lots: list[CCELLot], last_prices: dict[int, float]) -> tuple[dict[str, float], list[str]]:
    prices: dict[str, float] = {}
    unresolved: list[str] = []
    for lot in lots:
        price = _mark_price_for_security(last_prices, lot.ticker)
        if price is None:
            unresolved.append(lot.ticker)
            price = 0.0
        prices[lot.ticker] = price
    return prices, unresolved


def _valuation_diagnostics(lots: list[CCELLot], last_prices: dict[int, float]) -> dict[str, Any]:
    prices, unresolved = _mark_prices_for_lots(lots, last_prices)
    zero_marked = sorted({lot.ticker for lot in lots if prices.get(lot.ticker, 0.0) <= 0.0})
    return {
        "unresolved_mark_count": len(set(unresolved)),
        "unresolved_mark_tickers": sorted(set(unresolved)),
        "zero_mark_count": len(zero_marked),
        "zero_mark_tickers": zero_marked,
    }


def _passes_marketcap_floor(marketcap: float, min_marketcap: float) -> bool:
    """Sharadar DAILY marketcap is commonly reported in USD millions.

    The study config keeps thresholds in dollars. Accept either native-dollar
    values or million-dollar scaled values without changing the stored value,
    so same-unit valuation ratios remain untouched.
    """

    value = float(marketcap)
    floor = float(min_marketcap)
    if value >= floor:
        return True
    return value > 0 and value * 1_000_000.0 >= floor


def _last_price_on_or_before(series: pd.Series, date: pd.Timestamp) -> float | None:
    values = series.loc[series.index <= pd.Timestamp(date)]
    if values.empty:
        return None
    return float(values.iloc[-1])


def _dollar_adv(frame: pd.DataFrame, *, days: int) -> float | None:
    if frame.empty or "volume" not in frame.columns:
        return None
    rows = frame.tail(days)
    if rows.empty:
        return None
    return float((rows["price"].astype(float) * rows["volume"].astype(float)).mean())


def _zmap(values: dict[int, float | None]) -> dict[int, float]:
    clean = {key: float(value) for key, value in values.items() if value is not None and math.isfinite(float(value))}
    if not clean:
        return {}
    series = pd.Series(clean)
    std = float(series.std(ddof=0) or 0.0)
    if std <= 0:
        return {int(key): 0.0 for key in clean}
    mean = float(series.mean())
    return {int(key): float((value - mean) / std) for key, value in clean.items()}


def _none_low(value: float | None) -> float:
    return -math.inf if value is None else float(value)


def _float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def _parse_int(value: Any) -> int | None:
    parsed = _float(value)
    return int(parsed) if parsed is not None else None


def _safe_read_json(path: str | Path) -> dict[str, Any]:
    try:
        target = Path(path)
        if not target.exists():
            return {}
        payload = _read_json(target)
        return dict(payload) if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _acquisition_like(value: Any) -> bool:
    return bool(re.search("acquisition|buyout|going private|going-private|merger|takeover|cash", str(value or ""), flags=re.IGNORECASE))


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(value)).strip("_")


def _study_row(arm: str, payload: dict[str, Any], result_path: Path) -> dict[str, Any]:
    metrics = dict(payload.get("metrics") or {})
    row = {
        "arm": arm,
        "after_tax_terminal_wealth": metrics.get("after_tax_terminal_wealth"),
        "pre_tax_terminal_wealth": metrics.get("pre_tax_terminal_wealth"),
        "total_return": metrics.get("total_return"),
        "annualized_return": metrics.get("annualized_return"),
        "annualized_volatility": metrics.get("annualized_volatility"),
        "sharpe_ratio": metrics.get("sharpe_ratio"),
        "sortino_ratio": metrics.get("sortino_ratio"),
        "max_drawdown": metrics.get("max_drawdown"),
        "calmar_ratio": metrics.get("calmar_ratio"),
        "ulcer_index": metrics.get("ulcer_index"),
        "annualized_turnover": metrics.get("annualized_turnover"),
        "total_costs_paid": metrics.get("total_costs_paid"),
        "trade_count": metrics.get("trade_count"),
        "per_name_skew": metrics.get("per_name_skew"),
        "per_name_win_rate": metrics.get("per_name_win_rate"),
        "result_path": str(result_path),
    }
    if isinstance(payload.get("in_sample"), dict):
        row["in_sample"] = dict(payload.get("in_sample") or {})
    if isinstance(payload.get("out_of_sample"), dict):
        row["out_of_sample"] = dict(payload.get("out_of_sample") or {})
    return row


def _arm_sort_key(arm: str) -> tuple[int, str]:
    order = [*SELECTION_ARMS, *BENCHMARK_ARMS]
    return (order.index(arm) if arm in order else len(order), arm)


def _beats(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        (_float(left.get("after_tax_terminal_wealth"), -math.inf) or -math.inf) > (_float(right.get("after_tax_terminal_wealth"), math.inf) or math.inf)
        and (_float(left.get("calmar_ratio"), -math.inf) or -math.inf) > (_float(right.get("calmar_ratio"), math.inf) or math.inf)
        and (_float(left.get("ulcer_index"), math.inf) or math.inf) < (_float(right.get("ulcer_index"), -math.inf) or -math.inf)
    )


def _beats_oos(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        (_metric_value(left, "total_return", prefer_oos=True, default=-math.inf) or -math.inf) > (_metric_value(right, "total_return", prefer_oos=True, default=math.inf) or math.inf)
        and (_metric_value(left, "calmar_ratio", prefer_oos=True, default=-math.inf) or -math.inf) > (_metric_value(right, "calmar_ratio", prefer_oos=True, default=math.inf) or math.inf)
        and (_metric_value(left, "ulcer_index", prefer_oos=True, default=math.inf) or math.inf) < (_metric_value(right, "ulcer_index", prefer_oos=True, default=-math.inf) or -math.inf)
    )


def _metric_value(row: dict[str, Any], field: str, *, prefer_oos: bool = False, default: float | None = None) -> float | None:
    source = row.get("out_of_sample") if prefer_oos and isinstance(row.get("out_of_sample"), dict) else row
    return _float((source or {}).get(field), default)


def _relative_delta(left: dict[str, Any], right: dict[str, Any], *, prefer_oos: bool = False) -> dict[str, float | None]:
    return {
        "total_return_delta": _delta(_metric_value(left, "total_return", prefer_oos=prefer_oos), _metric_value(right, "total_return", prefer_oos=prefer_oos)),
        "calmar_delta": _delta(_metric_value(left, "calmar_ratio", prefer_oos=prefer_oos), _metric_value(right, "calmar_ratio", prefer_oos=prefer_oos)),
        "ulcer_delta": _delta(_metric_value(left, "ulcer_index", prefer_oos=prefer_oos), _metric_value(right, "ulcer_index", prefer_oos=prefer_oos)),
    }


def _delta(left: Any, right: Any) -> float | None:
    left_value = _float(left)
    right_value = _float(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def per_name_distribution(trades: list[dict[str, Any]], lots: list[CCELLot], final_prices: dict[str, float], label_by_perma: dict[int, str]) -> dict[str, Any]:
    pnl: dict[str, float] = {}
    for trade in trades:
        ticker = str(trade.get("ticker") or "")
        notional = float(trade.get("notional") or 0.0)
        cost = float(trade.get("costs_paid") or 0.0)
        if str(trade.get("side") or "").lower() == "buy":
            pnl[ticker] = pnl.get(ticker, 0.0) - notional - cost
        elif str(trade.get("side") or "").lower() == "sell":
            pnl[ticker] = pnl.get(ticker, 0.0) + notional - cost
    for lot in lots:
        pnl[lot.ticker] = pnl.get(lot.ticker, 0.0) + lot.quantity * float(final_prices.get(lot.ticker, 0.0))
    values = pd.Series(pnl, dtype="float64")
    distribution = [
        {
            "security": key,
            "ticker": label_by_perma.get(_security_to_perma(key), key) if key.startswith("P") else key,
            "pnl": value,
        }
        for key, value in sorted(pnl.items(), key=lambda item: item[1], reverse=True)
    ]
    positives = values[values > 0]
    return {
        "per_name_pnl_distribution": distribution,
        "per_name_skew": None if len(values) < 3 else float(values.skew()),
        "per_name_win_rate": None if values.empty else float((values > 0).mean()),
        "top_20_positive_pnl_share": None if positives.empty else float(positives.sort_values(ascending=False).head(max(1, math.ceil(len(positives) * 0.2))).sum() / positives.sum()),
    }


def _readiness_identifiers(store: SharadarStore, results: dict[str, dict[str, Any]]) -> list[int]:
    permatickers: set[int] = set()
    for payload in results.values():
        for rows in (payload.get("selection_history") or {}).values():
            for row in rows:
                if not isinstance(row, dict):
                    continue
                value = row.get("permaticker")
                if value is None:
                    continue
                permatickers.add(int(value))
    permas = sorted(permatickers)
    return permas[:200]


def _edgar_sample_permatickers(store: SharadarStore, sample_size: int) -> list[int]:
    tickers = store.read_meta_table("TICKERS")
    if tickers.empty or "permaticker" not in tickers.columns:
        return []
    rows = tickers.copy()
    if "category" in rows.columns:
        rows = rows.loc[rows["category"].astype(str).str.contains("Common Stock", case=False, na=False)]
    if "table" in rows.columns:
        rows = rows.loc[rows["table"].astype(str).str.upper().eq("SF1")]
    if "secfilings" in rows.columns:
        rows = rows.loc[rows["secfilings"].astype(str).str.contains("CIK=", na=False)].copy()
    rows["_delisted"] = rows.get("isdelisted", "").astype(str).str.upper().eq("Y") if "isdelisted" in rows.columns else False
    rows["_lastpricedate"] = pd.to_datetime(rows.get("lastpricedate"), errors="coerce") if "lastpricedate" in rows.columns else pd.NaT
    rows["_small_bucket"] = (
        rows.get("scalemarketcap", "")
        .astype(str)
        .str.contains("Nano|Micro|Small", case=False, regex=True, na=False)
        if "scalemarketcap" in rows.columns
        else False
    )
    groups = [
        rows.loc[rows["_delisted"] & rows["_small_bucket"]].sort_values(["_lastpricedate", "permaticker"], ascending=[False, True]),
        rows.loc[rows["_delisted"] & ~rows["_small_bucket"]].sort_values(["_lastpricedate", "permaticker"], ascending=[False, True]),
        rows.loc[~rows["_delisted"] & rows["_small_bucket"]].sort_values(["_lastpricedate", "permaticker"], ascending=[False, True]),
        rows.loc[~rows["_delisted"] & ~rows["_small_bucket"]].sort_values(["_lastpricedate", "permaticker"], ascending=[False, True]),
    ]
    out: list[int] = []
    seen: set[int] = set()
    while len(out) < sample_size and any(not group.empty for group in groups):
        for idx, group in enumerate(groups):
            if group.empty:
                continue
            row = group.iloc[0]
            groups[idx] = group.iloc[1:]
            perma = _int_or_none(row.get("permaticker"))
            if perma is not None and perma not in seen:
                seen.add(perma)
                out.append(perma)
                if len(out) >= sample_size:
                    break
    return out


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _basket_report_html(summary: dict[str, Any]) -> str:
    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    verdict = dict(summary.get("verdict") or {})
    title = "Basket Construction Study"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #111827; margin: 28px; line-height: 1.45; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 1200px; margin: 12px 0 24px; font-size: 13px; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 7px 9px; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, td.text, th.text {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    .banner {{ max-width: 1200px; border: 1px solid #f59e0b; background: #fffbeb; border-radius: 8px; padding: 12px 14px; }}
    .cards {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; max-width: 1200px; margin: 16px 0; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; }}
    .label {{ color: #6b7280; font-size: 12px; }}
    .value {{ font-size: 20px; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p>Window <code>{html.escape(str(summary.get("start")))}</code> to <code>{html.escape(str(summary.get("end")))}</code>; OOS starts <code>{html.escape(str(summary.get("oos_start")))}</code>. Snapshot <code>{html.escape(str(summary.get("data_snapshot_hash")))}</code>.</p>
  <div class="banner">Research-only. Gate status: <strong>{html.escape(str(summary.get("gate_status")))}</strong>. Verdict: <strong>{html.escape(str(verdict.get("status")))}</strong>. Production defaults changed: <strong>No</strong>.</div>
  <section class="cards">
    <div class="card"><div class="label">Best rule</div><div class="value">{html.escape(str(verdict.get("best_rule") or "n/a"))}</div></div>
    <div class="card"><div class="label">Data readiness</div><div class="value">{html.escape(str(summary.get("data_readiness")))}</div></div>
    <div class="card"><div class="label">EDGAR validation</div><div class="value">{html.escape(str((summary.get("edgar_validation") or {}).get("status")))}</div></div>
    <div class="card"><div class="label">Survivorship delta</div><div class="value">{_fmt_money((summary.get("survivorship_bias_delta") or {}).get("after_tax_terminal_wealth_delta"))}</div></div>
  </section>
  <h2>Readiness And Terminal Handling</h2>
  <p>Policy: `survivorship_free` requires adjusted price coverage plus explicit terminal handling for delisted names. Missing SF1 fundamentals are documented as selection exceptions; they do not fabricate fundamentals and do not remove failed names from price-based arms.</p>
  {_html_table(["Metric", "Value"], _readiness_table_rows(summary.get("readiness") or {}))}
  <h3>Terminal Value Disclosure</h3>
  <p>Ratified policy: failure, receivership, bankruptcy, and liquidation events mark common equity to <code>$0</code>; acquisition-like delistings mark to the last traded adjusted SEP price; unknown residuals branch by pre-delisting health and remain disclosed. Generic <code>ACTIONS.value</code> is not used as a payout.</p>
  {_html_table(["Metric", "Value"], _terminal_disclosure_rows(summary.get("terminal_value_disclosure") or {}))}
  <h4>Unknown Residuals And Review List</h4>
  {_html_table(["Ticker", "Permaticker", "Source", "Terminal Value", "Use Count", "Reason"], _unknown_residual_rows((summary.get("terminal_value_disclosure") or {}).get("unknown_residuals") or []))}
  {_html_table(["Ticker", "Permaticker", "Use Count", "Source", "Reason"], _held_review_rows((summary.get("terminal_value_disclosure") or {}).get("held_name_review_list") or []))}
  <h3>Terminal Event Use Counts</h3>
  {_html_table(["Ticker", "Permaticker", "Use Count", "Quantity", "Terminal Value", "Source", "Reason", "Arms"], _terminal_use_table_rows(summary.get("terminal_event_use_counts") or []))}
  <h3>Terminal Policy Before/After Trust Check</h3>
  <p>This table compares the prior campaign summary against this reason-dependent terminal-value rerun for C0b, A1-A4, and the synthesized S&P 500 benchmark.</p>
  {_html_table(["Arm", "Before Wealth", "After Wealth", "Wealth Delta", "Before CAGR", "After CAGR", "CAGR Delta", "Before Max DD", "After Max DD"], _terminal_policy_comparison_rows((summary.get("terminal_policy_comparison") or {}).get("comparison_rows") or []))}
  {_html_table(["Ticker", "Permaticker", "Use Count", "Source", "Previous Source", "Terminal Value", "Arms"], _last_price_flipped_rows((summary.get("terminal_policy_comparison") or {}).get("held_flipped_to_last_price") or []))}
  <h2>Survivorship Bias Delta</h2>
  <p><strong>Read this first.</strong> This is <code>C0_static_basket - C0b_static_pit</code>; it estimates how much hindsight selection inflated the current static basket baseline.</p>
  <pre>{html.escape(json.dumps(summary.get("survivorship_bias_delta") or {}, indent=2))}</pre>
  <h2>Results</h2>
  {_html_table(["Arm", "After-tax Wealth", "Pre-tax Wealth", "Return", "CAGR", "Sharpe", "Sortino", "Max DD", "Calmar", "Ulcer", "Turnover", "Costs", "Trades", "Skew", "Win Rate"], _result_rows(rows))}
  <h2>Per-Arm Verdicts</h2>
  {_html_table(["Arm", "Status", "Beats Static", "Beats Synth S&P 500", "OOS Return", "OOS Calmar", "OOS Ulcer"], _arm_verdict_rows(summary.get("arm_verdicts") or {}))}
  <h2>Benchmark Relative</h2>
  {_html_table(["Arm", "Benchmark", "Wealth Delta", "Return Delta", "CAGR Delta", "Calmar Delta", "Ulcer Delta", "OOS Return Delta", "OOS Calmar Delta", "OOS Ulcer Delta"], _basket_relative_table_rows(summary.get("benchmark_relative") or []))}
  <h2>Performance By Year</h2>
  {_html_table(["Year", *[str(row.get("arm")) for row in rows]], _yearly_rows(summary, rows))}
  <h2>Verdict</h2>
  <p>{html.escape(str(verdict.get("recommended_next_step") or ""))}</p>
  <h2>Reproduction</h2>
  <pre>{html.escape(str(summary.get("single_command") or ""))}</pre>
  <h2>Limitations</h2>
  <ul>{''.join(f"<li>{html.escape(str(item))}</li>" for item in summary.get("limitations") or [])}</ul>
</body>
</html>
"""


def _result_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    return [
        [
            str(row.get("arm")),
            _fmt_money(row.get("after_tax_terminal_wealth")),
            _fmt_money(row.get("pre_tax_terminal_wealth")),
            _fmt_pct(row.get("total_return")),
            _fmt_pct(row.get("annualized_return")),
            _fmt_num(row.get("sharpe_ratio")),
            _fmt_num(row.get("sortino_ratio")),
            _fmt_pct(row.get("max_drawdown")),
            _fmt_num(row.get("calmar_ratio")),
            _fmt_pct(row.get("ulcer_index")),
            _fmt_pct(row.get("annualized_turnover")),
            _fmt_money(row.get("total_costs_paid")),
            str(int(_float(row.get("trade_count"), 0.0) or 0)),
            _fmt_num(row.get("per_name_skew")),
            _fmt_pct(row.get("per_name_win_rate")),
        ]
        for row in rows
    ]


def _readiness_table_rows(readiness: dict[str, Any]) -> list[list[str]]:
    return [
        ["data_readiness", str(readiness.get("data_readiness"))],
        ["price_coverage_ratio", _fmt_pct(readiness.get("price_coverage_ratio"))],
        ["terminal_coverage_ratio", _fmt_pct(readiness.get("terminal_coverage_ratio"))],
        ["pit_fundamental_coverage_ratio", _fmt_pct(readiness.get("pit_fundamental_coverage_ratio"))],
        ["missing_price", ", ".join(str(item) for item in readiness.get("missing_price") or []) or "-"],
        ["missing_terminal", ", ".join(str(item) for item in readiness.get("missing_terminal") or []) or "-"],
        ["fundamental_exceptions", ", ".join(str(item) for item in readiness.get("fundamental_exceptions") or []) or "-"],
        ["reasons", ", ".join(str(item) for item in readiness.get("reasons") or []) or "-"],
    ]


def _terminal_use_table_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    if not rows:
        return [["-", "-", "0", "0", "-", "-", "-", "No terminal-value realizations in this window"]]
    return [
        [
            str(row.get("ticker") or ""),
            str(row.get("permaticker") or ""),
            str(row.get("use_count") or 0),
            _fmt_num(row.get("quantity")),
            _fmt_money(row.get("terminal_value")),
            str(row.get("terminal_value_source") or ""),
            str(row.get("terminal_value_reason") or ""),
            ", ".join(str(item) for item in row.get("arms") or []),
        ]
        for row in rows
    ]


def _terminal_disclosure_rows(disclosure: dict[str, Any]) -> list[list[str]]:
    return [
        ["policy", str(disclosure.get("policy") or "-")],
        ["category_breakdown", json.dumps(disclosure.get("category_breakdown") or {}, sort_keys=True)],
        ["held_category_breakdown", json.dumps(disclosure.get("held_category_breakdown") or {}, sort_keys=True)],
        ["failure_zero_count", str(disclosure.get("failure_zero_count") or 0)],
        ["last_price_terminal_count", str(disclosure.get("last_price_terminal_count") or 0)],
        ["held_last_price_terminal_count", str(disclosure.get("held_last_price_terminal_count") or 0)],
        ["unknown_residual_count", str(disclosure.get("unknown_residual_count") or 0)],
        ["conservative_default_count", str(disclosure.get("conservative_default_count") or 0)],
        ["held_name_review_count", str(disclosure.get("held_name_review_count") or 0)],
        ["held_terminal_changed_results_vs_prior_run", "yes" if disclosure.get("held_terminal_changed_results_vs_prior_run") else "no"],
    ]


def _held_review_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    if not rows:
        return [["-", "-", "0", "-", "No held unknown or missing-price terminal events require review."]]
    return [
        [
            str(row.get("ticker") or ""),
            str(row.get("permaticker") or ""),
            str(row.get("use_count") or 0),
            str(row.get("source") or ""),
            str(row.get("reason") or ""),
        ]
        for row in rows
    ]


def _unknown_residual_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    if not rows:
        return [["-", "-", "-", "-", "0", "No unknown residual terminal events."]]
    return [
        [
            str(row.get("ticker") or ""),
            str(row.get("permaticker") or ""),
            str(row.get("source") or ""),
            _fmt_money(row.get("terminal_value")),
            str(row.get("use_count") or 0),
            str(row.get("reason") or ""),
        ]
        for row in rows
    ]


def _terminal_policy_comparison_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    if not rows:
        return [["-", "-", "-", "-", "-", "-", "-", "-", "No prior summary was available for comparison."]]
    return [
        [
            str(row.get("arm") or ""),
            _fmt_money(row.get("before_after_tax_terminal_wealth")),
            _fmt_money(row.get("after_after_tax_terminal_wealth")),
            _fmt_money(row.get("after_tax_terminal_wealth_delta")),
            _fmt_pct(row.get("before_cagr")),
            _fmt_pct(row.get("after_cagr")),
            _fmt_pct(row.get("cagr_delta")),
            _fmt_pct(row.get("before_max_drawdown")),
            _fmt_pct(row.get("after_max_drawdown")),
        ]
        for row in rows
    ]


def _last_price_flipped_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    if not rows:
        return [["-", "-", "0", "-", "-", "-", "No held terminal events flipped from $0/default to last-price handling."]]
    return [
        [
            str(row.get("ticker") or ""),
            str(row.get("permaticker") or ""),
            str(row.get("use_count") or 0),
            str(row.get("source") or ""),
            str(row.get("previous_source") or ""),
            _fmt_money(row.get("terminal_value")),
            ", ".join(str(item) for item in row.get("arms") or []),
        ]
        for row in rows
    ]


def _arm_verdict_rows(verdicts: dict[str, Any]) -> list[list[str]]:
    rows: list[list[str]] = []
    for arm in SELECTION_ARMS:
        payload = dict(verdicts.get(arm) or {})
        if not payload:
            continue
        rows.append(
            [
                arm,
                str(payload.get("status") or ""),
                "yes" if payload.get("beats_static_basket") else "no",
                "yes" if payload.get("beats_synth_sp500") else "no",
                _fmt_pct(payload.get("oos_total_return")),
                _fmt_num(payload.get("oos_calmar_ratio")),
                _fmt_num(payload.get("oos_ulcer_index")),
            ]
        )
    return rows


def _basket_relative_table_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    out: list[list[str]] = []
    for row in rows:
        out.append(
            [
                str(row.get("arm") or ""),
                str(row.get("benchmark") or ""),
                _fmt_money(row.get("after_tax_terminal_wealth_delta")),
                _fmt_pct(row.get("total_return_delta")),
                _fmt_pct(row.get("cagr_delta")),
                _fmt_num(row.get("calmar_delta")),
                _fmt_num(row.get("ulcer_delta")),
                _fmt_pct(row.get("oos_total_return_delta")),
                _fmt_num(row.get("oos_calmar_delta")),
                _fmt_num(row.get("oos_ulcer_delta")),
            ]
        )
    return out


def _yearly_rows(summary: dict[str, Any], rows: list[dict[str, Any]]) -> list[list[str]]:
    returns = dict(summary.get("yearly_returns") or {})
    years = sorted({year for arm_returns in returns.values() if isinstance(arm_returns, dict) for year in arm_returns})
    arms = [str(row.get("arm")) for row in rows]
    return [[str(year), *[_fmt_pct((returns.get(arm) or {}).get(year)) for arm in arms]] for year in years]


def _fmt_money(value: Any) -> str:
    parsed = _float(value)
    return "n/a" if parsed is None else f"${parsed:,.0f}"


def _fmt_pct(value: Any) -> str:
    parsed = _float(value)
    return "n/a" if parsed is None else f"{parsed * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    parsed = _float(value)
    return "n/a" if parsed is None else f"{parsed:.2f}"
